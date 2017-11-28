'use strict';

const extend = require( 'extend' );
const pgp = require( 'pg-promise' )();

const Postgres_Driver = {
    init: async function() {
        this.db = pgp( this.options.db );

        if ( !this.options.table ) {
            throw new Error( 'Must specify a table!' );
        }

        if ( !this.options.table_create_sql ) {
            return;
        }

        await this.db.any( this.options.table_create_sql );
    },

    stop: async function() {
        this.db && this.db.$pool && await this.db.$pool.end();
        this.db = null;
    },

    put: async function( object, options ) {
        const data_keys = Object.keys( object ).sort().filter( key => key !== this.options.id_field );

        const exists = ( options && options.skip_exists_check ) ? false : await this.db.any( 'select 1 from ${table:name} where ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: object[ this.options.id_field ]
        } );

        if ( exists && exists.length ) {
            const update_statement = pgp.helpers.update( object, data_keys, this.options.table ) + ` WHERE "${ this.options.id_field }"='${ object[ this.options.id_field ] }'`;
            await this.db.none( update_statement );
            return;
        } else {
            const insert_statement = pgp.helpers.insert( object, null, this.options.table );
            await this.db.none( insert_statement );
            return;
        }
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