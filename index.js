'use strict';

const extend = require( 'extend' );
const pgp = require( 'pg-promise' )();

const connections = {};

function get_connection_id( db_info ) {
    return Object.keys( db_info ).sort().map( key => `${ key }:${ db_info[ key ] }` ).join( ';' );
}

const Postgres_Driver = {
    init: async function() {
        const connection_id = get_connection_id( this.options.db );
        let existing_connection = connections[ connection_id ];
        if ( !existing_connection ) {
            existing_connection = {
                connection: pgp( this.options.db ),
                refcount: 0
            };
            connections[ connection_id ] = existing_connection;
        }
        
        existing_connection.refcount++;
        this.db = existing_connection.connection;

        if ( !this.options.table ) {
            throw new Error( 'Must specify a table!' );
        }

        if ( !this.options.table_create_sql ) {
            return;
        }

        await this.db.any( this.options.table_create_sql );
    },

    stop: async function() {
        const connection_id = get_connection_id( this.options.db );
        const existing_connection = connections[ connection_id ];
        if ( !existing_connection ) {
            return;
        }

        existing_connection.refcount--;

        if ( existing_connection.refcount === 0 ) {
            this.db && this.db.$pool && await this.db.$pool.end();
            delete connections[ connection_id ];
        }

        this.db = null;
    },

    put: async function( object ) {
        const data_keys = Object.keys( object ).sort().filter( key => key !== this.options.id_field );
        const cs = new pgp.helpers.ColumnSet( data_keys, {
            table: this.options.table
        } );
        const upsert_statement = pgp.helpers.insert( object, cs ) + ` ON CONFLICT(${ this.options.id_field }) DO UPDATE SET ` + cs.assignColumns( {
            from: 'EXCLUDED'
        } );
        await this.db.none( upsert_statement );
    },

    get: async function( id ) {
        const rows = await this.db.any( 'SELECT * FROM ${table:name} WHERE ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: id
        } );

        const object = rows && rows.length ? rows[ 0 ] : null;

        return object;
    },

    del: async function( id ) {
        await this.db.any( 'DELETE FROM ${table:name} WHERE ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: id
        } );
    }
};

module.exports = {
    create: function( _options ) {
        const options = extend( true, {}, {
            readable: true,
            id_field: 'id',
            db: {
                host: 'localhost',
                port: 5432,
                ssl: false,
                idleTimeoutMillis: 30000
            },
            table_create_sql: null,
            processors: []
        }, _options );

        const instance = Object.assign( {}, Postgres_Driver );
        instance.options = options;

        return instance;
    }
};