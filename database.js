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
    dblog: {host: process.env.NODE4, port: process.env.PORT4, user: process.env.USER4, password: process.env.PASS4, database: process.env.DB4},
};

const STATUS = {
    START:      0,
    WRITE:      1,
    COMMITTING: 2,
    COMMITTED:  3,
    TERMINATE:  4,
    SUCCESS:    5,
};

const OPERATION = {
    INSERT: 'INSERT',
    UPDATE: 'UPDATE',
    DELETE: 'DELETE',
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

            // Connection
            node2_connection = sql.createConnection(database_configs.node2);

            // Query
            let statement = 'SELECT * FROM movies LIMIT 25 OFFSET 0';
            dataA = await runQuery(node2_connection, statement, 'READ');
            node2_connection.end();
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

                // Connection
                node1_connection = sql.createConnection(database_configs.node1);

                // Query
                let statement = 'SELECT * FROM movies WHERE year >= 1980 LIMIT 25 OFFSET 0';
                dataA = await runQuery(node1_connection, statement, 'READ');
                node1_connection.end();         
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

            // Connection
            node3_connection = sql.createConnection(database_configs.node3);

            // Query
            let statement = 'SELECT * FROM movies LIMIT 25 OFFSET 0';
            dataB = await runQuery(node3_connection, statement, 'READ');
            node3_connection.end();
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

                // Connection
                node1_connection = sql.createConnection(database_configs.node1);

                // Query
                let statement = 'SELECT * FROM movies WHERE year < 1980 LIMIT 25 OFFSET 0';
                dataB = await runQuery(node1_connection, statement, 'READ');
                node1_connection.end();            
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

            // Connection
            node2_connection = sql.createConnection(database_configs.node2);
            
            // Query
            let statement = 'SELECT * FROM movies WHERE `id` LIKE ? OR `name` LIKE ? OR `year` LIKE ? OR `rank` LIKE ? LIMIT 25 OFFSET 0';
            dataA = await runExecute(node2_connection, statement, values, 'READ');
            node2_connection.end();
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

                // Connection
                node1_connection = sql.createConnection(database_configs.node1);

                // Query
                let statement = 'SELECT * FROM (SELECT * FROM movies WHERE year >= 1980) as m WHERE `id` LIKE ? OR `name` LIKE ? OR `year` LIKE ? OR `rank` LIKE ? LIMIT 25 OFFSET 0';
                dataA = await runExecute(node1_connection, statement, values, 'READ');
                node1_connection.end();        
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

            // Connection
            node3_connection = sql.createConnection(database_configs.node3);

            // Query
            let statement = 'SELECT * FROM movies WHERE `id` LIKE ? OR `name` LIKE ? OR `year` LIKE ? OR `rank` LIKE ? LIMIT 25 OFFSET 0';
            dataB = await runExecute(node3_connection, statement, values, 'READ');
            node3_connection.end();
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

                // Connection
                node1_connection = sql.createConnection(database_configs.node1);

                // Query
                let statement = 'SELECT * FROM (SELECT * FROM movies WHERE year < 1980) as m WHERE `id` LIKE ? OR `name` LIKE ? OR `year` LIKE ? OR `rank` LIKE ? LIMIT 25 OFFSET 0';
                dataB = await runExecute(node1_connection, statement, values, 'READ');
                node1_connection.end();           
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
    postAddMovie: async (req, res, next) => {
        var node2_connection;
        var node3_connection;
        var values = [parseInt(req.body.id_text), req.body.name_text, parseInt(req.body.year_text), parseFloat(req.body.rank_text)];

        if (1980 <= parseInt(req.body.year_text)) {
            try {
                // Insert movie in replicate node 2.
                console.log('> Inserting data to node 2');

                if (req.crash_config.node2) {
                    throw new Error('> Simulated Crash!');
                }
                
                // Logging Values
                let log_values = [OPERATION.INSERT].concat(values).concat([STATUS.START, 2]);

                // Connection
                node2_connection = sql.createConnection(database_configs.node2);

                // Query
                let statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                await runExecuteLogs(node2_connection, statement, values, log_values, 'WRITE');
                node2_connection.end();
            } catch (err) {
                console.log('> Node 2 is unavailable! [ADD]');

                if (node2_connection != null) {
                    node2_connection.end();
                }
            };
        } else {
            try {
                // Insert movie in replicate node 3.
                console.log('> Inserting data to node 3');

                if (req.crash_config.node3) {
                    throw new Error('> Simulated Crash!');
                }
                
                // Logging Values
                let log_values = [OPERATION.INSERT].concat(values).concat([STATUS.START, 3]);

                // Connection
                node3_connection = sql.createConnection(database_configs.node3);

                // Query
                let statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                await runExecuteLogs(node3_connection, statement, values, log_values, 'WRITE');
                node3_connection.end();
            } catch (err) {
                console.log('> Node 3 is unavailable! [ADD]');
                console.log(err);

                if (node3_connection != null) {
                    node3_connection.end();
                }
            };
        }
    },
    postUpdateMovie: async (req, res, next) => {

    },
    postDeleteMovie: async (req, res, next) => {
        
    },
};

/** 
 *  Repetitive operations for MySQL queries.
*/
async function runQuery(connection, statement, type) {
    try {
        await connection.promise().query('SET AUTOCOMMIT = 0');
        await connection.promise().query('START TRANSACTION');
        await connection.promise().query(`LOCK TABLES movies ${type}`);
    
        let result_set = await connection.promise().query(statement);
    
        await connection.promise().query('COMMIT');
        await connection.promise().query('UNLOCK TABLES');
        return result_set[0];
    } catch (err) {
        throw err;
    }
};

/** 
 *  Repetitive operations for prepared MySQL queries w/ Logging.
*/
async function runExecuteLogs(connection, statement, values, log_values, type) {
    try {
        node1_connection = sql.createConnection(database_configs.node1);
        logs_connection = sql.createConnection(database_configs.dblog);

        await node1_connection.promise().query('SET AUTOCOMMIT = 0');
        await node1_connection.promise().query('START TRANSACTION');
        await node1_connection.promise().query(`LOCK TABLES movies ${type}`);        
        
        await connection.promise().query('SET AUTOCOMMIT = 0');
        await connection.promise().query('START TRANSACTION');
        await connection.promise().query(`LOCK TABLES movies ${type}`);

        // Insert Logs
        let log_statement = 'INSERT INTO logs (`operation`, `id`, `name`, `year`, `rank`, `status`, `node`) VALUES (?, ?, ?, ?, ?, ?, ?)';
        await logs_connection.promise().execute(log_statement, log_values);

        // Update Logs (Write)
        log_statement = 'UPDATE logs SET status = ? WHERE id = ? AND node = ?';
        let new_log_values = [STATUS.WRITE, log_values[1], log_values[6]];
        
        await logs_connection.promise().execute(log_statement, new_log_values);
        await node1_connection.promise().execute(statement, values);
        await connection.promise().execute(statement, values);

        // Update Logs (Committing)
        log_statement = 'UPDATE logs SET status = ? WHERE id = ? AND node = ?';
        new_log_values = [STATUS.COMMITTING, log_values[1], log_values[6]];

        await logs_connection.promise().execute(log_statement, new_log_values);
        await node1_connection.promise().query('COMMIT');
        await connection.promise().query('COMMIT');

        // Update Logs (Committed)
        log_statement = 'UPDATE logs SET status = ? WHERE id = ? AND node = ?';
        new_log_values = [STATUS.COMMITTED, log_values[1], log_values[6]];

        await logs_connection.promise().execute(log_statement, new_log_values);
        await node1_connection.promise().query('UNLOCK TABLES');
        await connection.promise().query('UNLOCK TABLES');

        console.log('INSERT SUCCESS');
    } catch (err) {
        throw err;
    }
};

/** 
 *  Repetitive operations for prepared MySQL queries.
*/
async function runExecute(connection, statement, values, type) {
    try {
        await connection.promise().query('SET AUTOCOMMIT = 0');
        await connection.promise().query('START TRANSACTION');
        await connection.promise().query(`LOCK TABLES movies ${type}`);
    
        let result_set = await connection.promise().execute(statement, values);
    
        await connection.promise().query('COMMIT');
        await connection.promise().query('UNLOCK TABLES');
        return result_set[0];
    } catch (err) {
        throw err;
    }
};


async function main() {
    console.log('Hello World!');
};

// main();
