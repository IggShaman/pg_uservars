
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


#define DEBUG 0

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


//
// Hash entry structure.
//
// Contains a copy of a key and a copy of data. Both strings are allocated in
// TopMemoryContext and freed when key is removed via pguser_delvar(..).
//
typedef struct {
    char* key;
    Size key_sz;
    char* val;
    Size val_sz;
} ukv_entry_t;


//
// hash accessors
//
static void ukv_hash_init ( void );
static ukv_entry_t* ukv_hash_get ( const char* key, Size len );
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
    
    #if DEBUG
    elog ( INFO, "pguser_setvar: set \"%s\" => \"%s\"", key, val );
    #endif
    ukv_hash_set ( key, strlen(key), val, strlen(val) );
    
    PG_RETURN_BOOL(true);
}


//
// Retrieve string by key
//
PG_FUNCTION_INFO_V1(pguser_getvar);
Datum pguser_getvar ( PG_FUNCTION_ARGS ) {
    
    char* key;
    ukv_entry_t* ent;
    
    if ( PG_ARGISNULL(0) )
    	PG_RETURN_NULL();
    
    key = PG_TEXT_GET_CSTR(PG_GETARG_TEXT_P(0));
    
    #if DEBUG
    elog ( INFO, "pguser_getvar: key=\"%s\"", key );
    #endif
    ent = ukv_hash_get ( key, strlen(key) );
    if ( ent ) {
	#if DEBUG
	elog ( INFO, "pguser_getvar: key=\"%s\" return \"%s\"", ent->key, ent->val );
	#endif
	PG_RETURN_TEXT_P(PG_CSTR_GET_TEXT(ent->val));
    }
    
    #if DEBUG
    elog ( INFO, "pguser_getvar: key=\"%s\" return NULL", key );
    #endif
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


static ukv_entry_t* ukv_hash_get ( const char* name, Size name_sz ) {
    
    ukv_entry_t key;
    ukv_entry_t* ent;
    
    if ( !ukv_hash )
    	return NULL;
    
    key.key = (char*)name;
    key.key_sz = name_sz;
    
    ent = (ukv_entry_t*)hash_search ( ukv_hash, &key, HASH_FIND, NULL );
    #if DEBUG
    elog ( INFO, "hash_get: key=\"%s\" size=%lu got ptr=%lu", name, name_sz, (Size)ent );
    #endif
    return ent;
}


//
// Hasher function for key part of ukv_entry_t.
//
static uint32 ukv_key_hash ( const void* key, Size keysize ) {
    int32_t hv = string_hash ( ((ukv_entry_t*)key)->key, ((ukv_entry_t*)key)->key_sz );
    #if DEBUG
    elog ( INFO, "computing hash for key=\"%s\" size=%lu hash=%d", ((ukv_entry_t*)key)->key, keysize, hv );
    #endif
    return hv;
}


//
// Key comparison function for key part of ukv_entry_t
//
static int ukv_key_match ( const void* ent1, const void* ent2, Size keysize ) {
    int match;
    
    Assert ( keysize == sizeof(ukv_entry_t) );
    
    do {
	if ( ((ukv_entry_t*)ent1)->key_sz != ((ukv_entry_t*)ent2)->key_sz ) {
	    match = false;
	    break;
	}
	
	match = memcmp ( ((ukv_entry_t*)ent1)->key, ((ukv_entry_t*)ent2)->key, ((ukv_entry_t*)ent1)->key_sz );
    } while ( false );
    
    #if DEBUG
    elog ( INFO, "\"%s\" match \"%s\" = %s",
	   ((ukv_entry_t*)ent1)->key,
	   ((ukv_entry_t*)ent2)->key,
	   0 == match ? "true" : "false" );
    #endif
    
    return match;
}


//
// Create copy of a memory region in TopMemoryContext
//
static char* p_memdup_top ( const void* ptr, Size sz ) {
    char* p2 = MemoryContextAlloc ( TopMemoryContext, sz+1 );
    if ( !p2 )
	elog ( ERROR, "out of memory" );
    
    memcpy ( p2, ptr, sz );
    p2[sz] = 0; // final \0, for strings
    return p2;
}


//
// Key copy: dup key part, leave value NULL.
//
// This callback is called by hash_search(..) when HASH_ENTER operation is used,
// and a new key is about to be insterted to our hash.
//
static void* ukv_key_copy ( void* dest, const void* src, Size sz ) {
    ((ukv_entry_t*)dest)->key = p_memdup_top ( ((ukv_entry_t*)src)->key, ((ukv_entry_t*)src)->key_sz );
    ((ukv_entry_t*)dest)->key_sz = ((ukv_entry_t*)src)->key_sz;
    ((ukv_entry_t*)dest)->val = NULL;
    ((ukv_entry_t*)dest)->val_sz = 0;
    
    #if DEBUG
    elog ( INFO, "copied key=\"%s\" sz=%lu", ((ukv_entry_t*)dest)->key, ((ukv_entry_t*)dest)->key_sz );
    #endif
    
    return dest;
}


//
// ukv_hash initialization.
//
static void ukv_hash_init ( void ) {
    HASHCTL ctl;
    
    ctl.keysize = sizeof(ukv_entry_t);
    ctl.entrysize = sizeof(ukv_entry_t);
    ctl.hash = ukv_key_hash;
    ctl.match = ukv_key_match;
    ctl.keycopy = ukv_key_copy;
    
    ukv_hash = hash_create ( "user key/value hash", MAX_EXPECTED_VARS, &ctl,
			     HASH_FUNCTION|HASH_ELEM|HASH_COMPARE|HASH_KEYCOPY );
    
    if ( NULL == ukv_hash )
	ereport ( ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")) );
}


//
// Adds new value to hash, or sets a new value for pre-existing key.
//
static void ukv_hash_set ( const char* key_name, Size key_sz, const char* new_val, Size new_val_sz ) {
    
    ukv_entry_t key;
    ukv_entry_t* ent;
    bool found;
    
    if ( !ukv_hash )
	ukv_hash_init();
    
    key.key = (char*)key_name;
    key.key_sz = key_sz;
    key.val = NULL;
    key.val_sz = 0;
    
    ent = (ukv_entry_t*)hash_search ( ukv_hash, &key, HASH_ENTER, &found );
    
    if ( NULL == ent )
	ereport ( ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    
    if ( found ) {
	#if DEBUG
	elog ( INFO, "key \"%s\" found", key_name );
	#endif
	if ( ent->val )
	    pfree ( ent->val );
	
    } else {
	#if DEBUG
	elog ( INFO, "key \"%s\" not found, inserting", key_name );
	#endif
    }
    
    // key and key_sz are copied via ukv_key_copy
    //ent->key = p_memdup_top ( key_name, key_sz );
    //ent->key_sz = key_sz;
    ent->val = p_memdup_top ( new_val, new_val_sz );
    ent->val_sz = new_val_sz;
    
    #if DEBUG
    elog ( INFO, "ukv_hash_set: key=\"%s\" value set to \"%s\" len=%lu; new_val=\"%s\" new_val_len=%lu",
	   key_name, ent->val, ent->val_sz, new_val, new_val_sz );
    #endif
}


//
// Removes key/value pair from ukv_hash and frees memory.
//
static void ukv_hash_del ( const char* name, Size name_sz ) {
    
    ukv_entry_t key;
    ukv_entry_t* ent;
    bool found;
    
    if ( !ukv_hash )
    	return; // no hash => no keys
    
    key.key = (char*)name;
    key.key_sz = name_sz;
    
    ent = (ukv_entry_t*)hash_search ( ukv_hash, &key, HASH_REMOVE, &found );
    
    if ( ent ) {
    	pfree ( ent->key );
    	pfree ( ent->val );
    }
    
    #if DEBUG
    elog ( INFO, "ukv_hash_del: key=\"%s\"", name );
    #endif
}
