'use strict';

const Postgres_Driver = require( '../index.js' );
const tape = require( 'tape-async' );

tape( 'API: imports properly', t => {
    t.ok( Postgres_Driver, 'module exports' );
    t.equal( Postgres_Driver && typeof Postgres_Driver.create, 'function', 'exports create()' );
    t.end();
} );

tape( 'API: API is correct on driver instance', t => {

    const postgres_driver = Postgres_Driver.create();

    t.ok( postgres_driver, 'got driver instance' );

    t.equal( postgres_driver && typeof postgres_driver.init, 'function', 'exports init' );
    t.equal( postgres_driver && typeof postgres_driver.stop, 'function', 'exports stop' );
    t.equal( postgres_driver && typeof postgres_driver.put, 'function', 'exports put' );
    t.equal( postgres_driver && typeof postgres_driver.get, 'function', 'exports get' );
    t.equal( postgres_driver && typeof postgres_driver.del, 'function', 'exports del' );

    t.end();
} );