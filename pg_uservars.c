
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"

PG_MODULE_MAGIC;


// max expected number of user variables
#define MAX_EXPECTED_VARS 32


//
// exported
//
extern Datum pguser_setvar(PG_FUNCTION_ARGS);
extern Datum pguser_getvar(PG_FUNCTION_ARGS);
extern Datum pguser_delvar(PG_FUNCTION_ARGS);


//
// hash with user keys/values
//
static HTAB* ukv_hash = NULL;
typedef struct {
    const char* key;
    Size len;
} ukv_key_t;

typedef struct {
    char* val;
    Size len;
} ukv_value_t;


//
// hash accessors
//
static void ukv_hash_init ( void );
static ukv_value_t* ukv_hash_get ( const char* key, Size len );
static void ukv_hash_set ( const char* key, Size key_len, const char* val, Size val_len );
static void ukv_hash_del ( const char* key, Size len );


#define PG_CSTR_GET_TEXT(cstrp) DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(cstrp)))
#define PG_TEXT_GET_CSTR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))


//
// set new variable
//
PG_FUNCTION_INFO_V1(pguser_setvar);
Datum pguser_setvar ( PG_FUNCTION_ARGS ) {
    
    char* key;
    char* val;
    
    if ( PG_ARGISNULL(0) || PG_ARGISNULL(1) )
	PG_RETURN_NULL();
    
    key = PG_TEXT_GET_CSTR(PG_GETARG_TEXT_P(0));
    val = PG_TEXT_GET_CSTR(PG_GETARG_TEXT_P(1));
    
    //elog ( INFO, "set \"%s\" => \"%s\"", key, val );
    ukv_hash_set ( key, strlen(key), val, strlen(val) );
    
    PG_RETURN_BOOL(true);
}


//
// Retrieve string by key
//
PG_FUNCTION_INFO_V1(pguser_getvar);
Datum pguser_getvar ( PG_FUNCTION_ARGS ) {
    
    char* key;
    ukv_value_t* val;
    
    if ( PG_ARGISNULL(0) )
    	PG_RETURN_NULL();
    
    key = PG_TEXT_GET_CSTR(PG_GETARG_TEXT_P(0));
    
    //elog ( INFO, "get \"%s\"", key );
    val = ukv_hash_get ( key, strlen(key) );
    if ( val )
	PG_RETURN_TEXT_P(PG_CSTR_GET_TEXT(val->val));
    else
	PG_RETURN_NULL();
}


//
// Delete key/value
//
PG_FUNCTION_INFO_V1(pguser_delvar);
Datum pguser_delvar ( PG_FUNCTION_ARGS ) {
    
    char* key;
    
    if ( PG_ARGISNULL(0) )
    	PG_RETURN_NULL();
    
    key = PG_TEXT_GET_CSTR(PG_GETARG_TEXT_P(0));
    ukv_hash_del ( key, strlen(key) );
    PG_RETURN_BOOL(true);
}


static ukv_value_t* ukv_hash_get ( const char* name, Size name_len ) {
    
    ukv_key_t key;
    ukv_value_t* val;
    
    if ( !ukv_hash )
    	return NULL;
    
    key.key = name;
    key.len = name_len;
    
    val = (ukv_value_t*)hash_search ( ukv_hash, &key, HASH_FIND, NULL );
    //elog ( INFO, "name=\"%s\" size=%lu got value=%lu", name, name_len, (Size)val );
    return val;
}


static uint32 ukv_key_hash ( const void* key, Size keysize ) {
    int32_t hv = string_hash ( ((ukv_key_t*)key)->key, ((ukv_key_t*)key)->len );
    //elog ( INFO, "computing hash for key=\"%s\" size=%d hash=%d", ((ukv_key_t*)key)->key, keysize, hv );
    return hv;
}


// key comparison function
static int ukv_key_match ( const void* key1, const void* key2, Size keysize ) {
    int match;
    
    Assert ( keysize == sizeof(ukv_key_t) );
    
    do {
	if ( ((ukv_key_t*)key1)->len != ((ukv_key_t*)key2)->len ) {
	    match = false;
	    break;
	}
	
	match = (0 == memcmp ( ((ukv_key_t*)key1)->key, ((ukv_key_t*)key2)->key, ((ukv_key_t*)key1)->len ));
    } while ( false );
    
    //elog ( INFO, "\"%s\" match \"%s\" = %s",
    //((ukv_key_t*)key1)->key,
    //((ukv_key_t*)key2)->key,
    //match ? "true" : "false" );
    
    return match;
}


static void ukv_hash_init ( void ) {
    HASHCTL ctl;
    
    ctl.keysize = sizeof(ukv_key_t);
    ctl.entrysize = sizeof(ukv_value_t);
    ctl.hash = ukv_key_hash;
    ctl.match = ukv_key_match;
    
    ukv_hash = hash_create ( "user key/value hash", MAX_EXPECTED_VARS, &ctl,
			     HASH_FUNCTION|HASH_ELEM|HASH_COMPARE );
    
    if ( NULL == ukv_hash )
	ereport ( ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")) );
}


static char* p_memdup_top ( const void* ptr, Size sz ) {
    void* p2 = MemoryContextAlloc ( TopMemoryContext, sz );
    if ( !p2 )
	elog ( ERROR, "out of memory" );
    
    memcpy ( p2, ptr, sz );
    return p2;
}


static void ukv_hash_set ( const char* name, Size name_len, const char* new_val, Size new_val_len ) {
    
    ukv_key_t key;
    ukv_value_t* val;
    bool found;
    
    //elog ( INFO, "setting variable name=\"%s\" size=%d value=\"%s\" size=%d", name, name_len, new_val, new_val_len );
    
    if ( !ukv_hash )
	ukv_hash_init();
    
    key.key = name;
    key.len = name_len;
    
    val = (ukv_value_t*)hash_search ( ukv_hash, &key, HASH_ENTER, &found );
    
    if ( NULL == val )
	ereport ( ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    
    if ( found ) {
	if ( val->val )
	    pfree ( val->val );
	
	//ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("duplicate string name")));
    }
    
    val->val = p_memdup_top ( new_val, new_val_len );
    //elog ( INFO, "value set to \"%s\"", val->val );
}


static void ukv_hash_del ( const char* name, Size name_len ) {
    
    ukv_key_t key;
    ukv_value_t* val;
    bool found;
    
    if ( !ukv_hash )
    	return; // no hash => no keys
    
    key.key = name;
    key.len = name_len;
    
    val = (ukv_value_t*)hash_search ( ukv_hash, &key, HASH_REMOVE, &found );
    
    if ( val )
    	pfree ( val->val );
}
