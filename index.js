'use strict';

const extend = require( 'extend' );
const pgp = require( 'pg-promise' )();

const Postgres_Driver = {
    init: async function( options ) {
        this.options = options;

        this.db = pgp( this.options.db );

        if ( !this.options.table ) {
            throw new Error( 'Must specify a table!' );
        }

        if ( !this.options.mapper ) {
            throw new Error( 'Must specify a mapper!' );
        }

        if ( !this.options.unmapper ) {
            throw new Error( 'Must specify an unmapper!' );
        }

        if ( !this.options.table_create_sql ) {
            return;
        }

        await this.db.any( this.options.table_create_sql );
    },

    stop: async function() {
        await pgp.end();
    },

    put: async function( object ) {
        const mapped_object = this.options.mapper( object );
        const mapped_object_data_keys = Object.keys( mapped_object ).sort().filter( key => key !== this.options.id_field );

        const exists = await this.db.one( 'select 1 from ${table:name} where ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: mapped_object[ this.options.id_field ]
        } );

        if ( exists ) {
            const update_statement = pgp.helpers.update( mapped_object, mapped_object_data_keys, this.options.table ) + ` WHERE "${ this.options.id_field }"="${ mapped_object[ this.options.id_field ] }"`;
            return await this.db.one( update_statement );
        } else {
            const insert_statement = pgp.helpers.insert( mapped_object, null, this.options.table );
            return await this.db.one( insert_statement );
        }
    },

    get: async function( id ) {
        const mapped_result = await this.db.one( 'SELECT * FROM ${table:name} WHERE ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: id
        } );

        const result = mapped_result ? this.options.unmapper( mapped_result ) : null;
        return result;
    },

    del: async function( id ) {
        await this.db.none( 'DELETE FROM ${table:name} WHERE ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: id
        } );
    }
};

module.exports = {
    create: function( _options ) {
        const options = extend( true, {
            id_field: 'id',
            readable: true,
            db: {
                user: null,
                database: null,
                password: null,
                host: 'localhost',
                port: 5432,
                ssl: false,
                idleTimeoutMillis: 30000,
                max: 20
            },
            table_create_sql: null
        }, _options );

        const instance = Object.assign( {}, Postgres_Driver );
        instance.options = options;

        return instance;
    }
};