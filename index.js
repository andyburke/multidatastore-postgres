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
        const processed = await this.options.processors.map( processor => processor.serialize ).reduce( async ( _object, serialize ) => {
            if ( !serialize ) {
                return _object;
            }

            return await serialize( _object );
        }, object );

        const data_keys = Object.keys( processed ).sort().filter( key => key !== this.options.id_field );

        const exists = ( options && options.skip_exists_check ) ? false : await this.db.any( 'select 1 from ${table:name} where ${id_field:name}=${id}', {
            table: this.options.table,
            id_field: this.options.id_field,
            id: processed[ this.options.id_field ]
        } );

        if ( exists && exists.length ) {
            const update_statement = pgp.helpers.update( processed, data_keys, this.options.table ) + ` WHERE "${ this.options.id_field }"='${ processed[ this.options.id_field ] }'`;
            await this.db.none( update_statement );
            return;
        } else {
            const insert_statement = pgp.helpers.insert( processed, null, this.options.table );
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

        const processed = rows && rows.length ? rows[ 0 ] : null;
        const object = await this.options.processors.map( processor => processor.deserialize ).reduceRight( async ( _object, deserialize ) => {
            if ( !deserialize ) {
                return _object;
            }

            return await deserialize( _object );
        }, processed );

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