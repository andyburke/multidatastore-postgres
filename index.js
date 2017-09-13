'use strict';

const extend = require( 'extend' );
const pgp = require( 'pg-promise' )();

const Postgres_Driver = {
    init: async function() {
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
        this.db && this.db.$pool && await this.db.$pool.end();
        this.db = null;
    },

    put: async function( object ) {
        const mapped_object = await this.options.mapper( object );
        const mapped_object_data_keys = Object.keys( mapped_object ).sort().filter( key => key !== this.options.id_field );

        const exists = await this.db.any( 'select 1 from ${table:name} where ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: mapped_object[ this.options.id_field ]
        } );

        if ( exists && exists.length ) {
            const update_statement = pgp.helpers.update( mapped_object, mapped_object_data_keys, this.options.table ) + ` WHERE "${ this.options.id_field }"='${ mapped_object[ this.options.id_field ] }'`;
            await this.db.none( update_statement );
            return;
        } else {
            const insert_statement = pgp.helpers.insert( mapped_object, null, this.options.table );
            await this.db.none( insert_statement );
            return;
        }
    },

    get: async function( id ) {
        const mapped_result = await this.db.any( 'SELECT * FROM ${table:name} WHERE ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: id
        } );

        const result = mapped_result && mapped_result.length ? await this.options.unmapper( mapped_result[ 0 ] ) : null;
        return result;
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
            table_create_sql: null
        }, _options );

        const instance = Object.assign( {}, Postgres_Driver );
        instance.options = options;

        return instance;
    }
};