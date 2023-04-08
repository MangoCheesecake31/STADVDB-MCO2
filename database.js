/*
    Node 1 (Central Node)
    Node 2 (Replicated Node, After + 1980)
    Node 3 (Replicated Node, Before  1980)
*/
const { config } = require('dotenv');
const sql = require('mysql2');
require('dotenv').config();

const database_configs = {
    node1: {host: process.env.NODE1, port: process.env.PORT1, user: process.env.USER1, password: process.env.PASS1, database: process.env.DB1},
    node2: {host: process.env.NODE2, port: process.env.PORT2, user: process.env.USER2, password: process.env.PASS2, database: process.env.DB2},
    node3: {host: process.env.NODE3, port: process.env.PORT3, user: process.env.USER3, password: process.env.PASS3, database: process.env.DB3},
    dblog: {},
};

module.exports = {
    getMovie: async (req, res, next) => {
        var node1_connection;
        var node2_connection;
        var node3_connection;
        var dataA, dataB;

        try {
            // Fetch data from replicate node 2.
            console.log('> Fetching data from node 2');

            if (req.crash_config.node2) {
                throw new Error('> Simulated Crash!');
            }

            node2_connection = sql.createConnection(database_configs.node2);
            let statement = 'SELECT * FROM movies LIMIT 25 OFFSET 0';                           // TODO: Parameterize Offset

            let result = await new Promise((resolve, reject) => {
                node2_connection.query(statement, (err, results, fields) => {
                    (results) ? resolve(results) : reject(err);      
                });
            });
            node2_connection.end();
            dataA = result;
        } catch (err) {
            console.log('> Node 2 is unavailable!');
            if (node2_connection != null) {
                node2_connection.end();
            }

            try {
                // Fetch node 2 data from central node 1 instead.
                console.log('> Fetching node 2 data from node 1');

                if (req.crash_config.node1) {
                    throw new Error('> Simulated Crash!');
                }

                let statement = 'SELECT * FROM movies WHERE year >= 1980 LIMIT 25 OFFSET 0';    // TODO: Parameterize Offset
                node1_connection = sql.createConnection(database_configs.node1);

                let result = await new Promise((resolve, reject) => {
                    node1_connection.query(statement, (err, results, fields) => {
                        (results) ? resolve(results) : reject(err);      
                    });  
                });    
                node1_connection.end();
                dataA = result;          
            } catch (err) {
                console.log('> Node 1 is unavailable!');
                if (node1_connection != null) {
                    node1_connection.end();
                }     

                // Throw error
                var error = new Error('Service Unavailable');
                error.status = 503;
                return next(error);
            }
        }

        try {
            // Fetch data from replicate node 3.
            console.log('> Fetching data from node 3');

            if (req.crash_config.node3) {
                throw new Error('> Simulated Crash!');
            }

            node3_connection = sql.createConnection(database_configs.node3);
            let statement = 'SELECT * FROM movies LIMIT 25 OFFSET 0';                           // TODO: Parameterize Offset

            let result = await new Promise((resolve, reject) => {
                node3_connection.query(statement, (err, results, fields) => {
                    (results) ? resolve(results) : reject(err);     
                });
            });
            node3_connection.end();
            dataB = result;  
        } catch (err) {
            console.log('> Node 3 is unavailable!');
            if (node3_connection != null) {
                node3_connection.end();
            }

            try {
                // Fetch node 3 data from central node 1 instead.
                console.log('> Fetching node 3 data from node 1');

                if (req.crash_config.node1) {
                    throw new Error('> Simulated Crash!');
                }

                let statement = 'SELECT * FROM movies WHERE year < 1980 LIMIT 25 OFFSET 0';     // TODO: Parameterize Offset
                node1_connection = sql.createConnection(database_configs.node1);

                let result = await new Promise((resolve, reject) => {
                    node1_connection.query(statement, (err, results, fields) => {
                        (results) ? resolve(results) : reject(err);      
                    });  
                });    
                node1_connection.end();
                dataB = result;             
            } catch (err) {
                console.log('> Node 1 is unavailable!');
                if (node1_connection != null) {
                    node1_connection.end();
                }     

                // Throw error
                var error = new Error('Service Unavailable');
                error.status = 503;
                return next(error);
            }
        }
        console.log("> Rendering page");
        res.render('index', {table_data: [dataA, dataB]});
    },
    getMovieSearch: async (req, res, next) => {
        var node1_connection;
        var node2_connection;
        var node3_connection;
        var dataA, dataB;
        var values = [`%${req.body.search_text}%`, `%${req.body.search_text}%`, `%${req.body.search_text}%`, `%${req.body.search_text}%`];

        try {
            // Fetch data from replicate node 2.
            console.log('> Fetching data from node 2');

            if (req.crash_config.node2) {
                throw new Error('> Simulated Crash!');
            }

            node2_connection = sql.createConnection(database_configs.node2);
            let statement = 'SELECT * FROM movies WHERE `id` LIKE ? OR `name` LIKE ? OR `year` LIKE ? OR `rank` LIKE ? LIMIT 25 OFFSET 0';

            let result = await new Promise((resolve, reject) => {
                node2_connection.execute(statement, values, (err, results, fields) => {
                    (results) ? resolve(results) : reject(err);    
                });
            });
            node2_connection.end();
            dataA = result;
        } catch (err) {
            console.log('> Node 2 is unavailable!');
            if (node2_connection != null) {
                node2_connection.end();
            }

            try {
                // Fetch node 2 data from central node 1 instead.
                console.log('> Fetching node 2 data from node 1');

                if (req.crash_config.node1) {
                    throw new Error('> Simulated Crash!');
                }

                node1_connection = sql.createConnection(database_configs.node1);
                let statement = 'SELECT * FROM (SELECT * FROM movies WHERE year >= 1980) as m WHERE `id` LIKE ? OR `name` LIKE ? OR `year` LIKE ? OR `rank` LIKE ? LIMIT 25 OFFSET 0';
                
                let result = await new Promise((resolve, reject) => {
                    node1_connection.execute(statement, values, (err, results, fields) => {
                        (results) ? resolve(results) : reject(err);      
                    });  
                });    
                node1_connection.end();
                dataA = result;          
            } catch (err) {
                console.log('> Node 1 is unavailable!');
                if (node1_connection != null) {
                    node1_connection.end();
                }     

                // Throw error
                var error = new Error('Service Unavailable');
                error.status = 503;
                return next(error);
            }
        }

        try {
            // Fetch data from replicate node 3.
            console.log('> Fetching data from node 3');

            if (req.crash_config.node3) {
                throw new Error('> Simulated Crash!');
            }

            node3_connection = sql.createConnection(database_configs.node3);
            let statement = 'SELECT * FROM movies WHERE `id` LIKE ? OR `name` LIKE ? OR `year` LIKE ? OR `rank` LIKE ? LIMIT 25 OFFSET 0';

            let result = await new Promise((resolve, reject) => {
                node3_connection.execute(statement, values, (err, results, fields) => {
                    (results) ? resolve(results) : reject(err);     
                });
            });
            node3_connection.end();
            dataB = result;  
        } catch (err) {
            console.log('> Node 3 is unavailable!');
            if (node3_connection != null) {
                node3_connection.end();
            }

            try {
                // Fetch node 3 data from central node 1 instead.
                console.log('> Fetching node 3 data from node 1');

                if (req.crash_config.node1) {
                    throw new Error('> Simulated Crash!');
                }

                let statement = 'SELECT * FROM (SELECT * FROM movies WHERE year < 1980) as m WHERE `id` LIKE ? OR `name` LIKE ? OR `year` LIKE ? OR `rank` LIKE ? LIMIT 25 OFFSET 0';
                node1_connection = sql.createConnection(database_configs.node1);

                let result = await new Promise((resolve, reject) => {
                    node1_connection.execute(statement, values, (err, results, fields) => {
                        (results) ? resolve(results) : reject(err);      
                    });  
                });    
                node1_connection.end();
                dataB = result;             
            } catch (err) {
                console.log('> Node 1 is unavailable!');
                if (node1_connection != null) {
                    node1_connection.end();
                }     

                // Throw error
                var error = new Error('Service Unavailable');
                error.status = 503;
                return next(error);
            }
        }
        console.log("> Rendering page");
        res.render('index', {table_data: [dataA, dataB]});
    },
};


async function main() {


    // node1_connection = sql.createConnection(database_configs.node3);

    // console.log(database_configs.node3);

    const data = await getMovie();
    console.log(data[0].length);
    console.log(data[1].length);


    // node1_connection.query('SELECT * FROM movies LIMIT 20', (err, results, fields) => {     
    //     console.log(results);
    // });
};

// main();
