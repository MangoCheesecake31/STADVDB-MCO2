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
        var node1_connection;
        var node2_connection;
        var node3_connection;
        var values = [parseInt(req.body.id_text), req.body.name_text, parseInt(req.body.year_text), parseFloat(req.body.rank_text)];
        var replicateFlag = false;

        try {
            console.log('> Inserting data to node 1');
            if (req.crash_config.node1) { throw new Error('> Simulated Crash!'); }

            // Connection
            node1_connection = sql.createConnection(database_configs.node1);

            // Query
            let statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
            await runExecuteLogs(node1_connection, statement, values, [OPERATION.INSERT, STATUS.START, 1].concat(values), 'WRITE', 0);

            node1_connection.end();
            replicateFlag = true;
            console.log('> Insert Movie Node 1 Success!');
        } catch (err) {
            console.log('> Node 1 is unavailable!');
            if (node1_connection != null) { node1_connection.end(); }
        }
        
        // Replicate
        if (!replicateFlag) { return; }
        if (values[2] >= 1980) {                            // Node 2
            try {
                console.log('> Replicating data into Node 2!');
                if (req.crash_config.node2) { throw new Error('> Simulated Crash!'); }

                // Connection
                node2_connection = sql.createConnection(database_configs.node2);

                // Query
                let statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                await runExecuteLogs(node2_connection, statement, values, [OPERATION.INSERT, STATUS.START, 2].concat(values), 'WRITE', 0);

                node2_connection.end();
                console.log('> Insert Movie Node 2 Success!');
            } catch (err) {
                console.log('> Node 2 is unavailable! [INSERT-REP]');
                if (node2_connection != null) { node2_connection.end(); }
            }   
        } else {                                            // Node 3
            try {
                console.log('> Replicating data into Node 3!');
                if (req.crash_config.node3) { throw new Error('> Simulated Crash!'); }

                // Connection
                node3_connection = sql.createConnection(database_configs.node3);

                // Query
                let statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                await runExecuteLogs(node3_connection, statement, values, [OPERATION.INSERT, STATUS.START, 3].concat(values), 'WRITE', 0);

                node3_connection.end();
                console.log('> Insert Movie Node 3 Success!');
            } catch (err) {
                console.log('> Node 3 is unavailable! [INSERT-REP]');
                if (node3_connection != null) { node3_connection.end(); }
            }
        }
    },
    postUpdateMovie: async (req, res, next) => {
        var node1_connection;
        var node2_connection;
        var node3_connection;
        var values = [parseInt(req.body.editid_text), req.body.editname_text, parseInt(req.body.edityear_text), parseFloat(req.body.editrank_text)];
        var originalYear = parseInt(req.body.orig_year);
        var replicateFlag = false;

        if (values[3] == NaN) { values[3] == null };

        console.log(values);
        console.log(values[1], values[2], values[3], values[0]);


        try {
            console.log('> Updating data from node 1');
            if (req.crash_config.node1) { throw new Error('> Simulated Crash!'); }

            // Connection
            node1_connection = sql.createConnection(database_configs.node1);

            // Query
            let statement = 'UPDATE movies SET `name` = ?, `year` = ?, `rank` = ? WHERE id = ?';
            await runExecuteLogs(node1_connection, statement, [values[1], values[2], values[3], values[0]], [OPERATION.UPDATE, STATUS.START, 1].concat(values), 'WRITE', 3);

            node1_connection.end();
            replicateFlag = true;
            console.log('> Update Movie Node 1 Success!');
        } catch (err) {
            console.log('> Node 1 is unavailable!');
            if (node1_connection != null) { node1_connection.end(); }
        }

        // Replicate
        if (!replicateFlag) { return; }
        if (originalYear >= 1980) {                            // Node 2
            try {
                console.log('> Replicating data into Node 2!');
                if (req.crash_config.node2) { throw new Error('> Simulated Crash!'); }

                // Connection
                node2_connection = sql.createConnection(database_configs.node2);

                // Query
                if (!(values[2] >= 1980)) {
                    console.log('> Swapping');
                    try {
                        // Connection
                        node3_connection = sql.createConnection(database_configs.node3);

                        // Query
                        let insert_statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                        await runExecuteLogs(node3_connection, insert_statement, values, [OPERATION.INSERT, STATUS.START, 3].concat(values), 'WRITE', 0);
                        let delete_statement = 'DELETE FROM movies WHERE id = ?';
                        await runExecuteLogs(node2_connection, delete_statement, [values[0]], [OPERATION.DELETE, STATUS.START, 2].concat(values), 'WRITE', 0);

                        node2_connection.end();
                        node3_connection.end();
                        console.log('> Update Movie SWAP Node 2 & 3 Success!');
                    } catch (err) {
                        console.log('> Node 3 is unavailable! [UPDATE-REP]');
                        if (node3_connection != null) { node3_connection.end(); }
                    }
                } else {
                    let statement = 'UPDATE movies SET `name` = ?, `year` = ?, `rank` = ? WHERE id = ?';
                    await runExecuteLogs(node2_connection, statement, [values[1], values[2], values[3], values[0]], [OPERATION.UPDATE, STATUS.START, 2].concat(values), 'WRITE', 3);
            
                    node2_connection.end();
                    console.log('> Update Movie Node 2 Success!');
                }    
            } catch (err) {
                console.log('> Node 2 is unavailable! [UPDATE-REP]');
                if (node2_connection != null) { node2_connection.end(); }
            }   
        } else {                                            // Node 3
            try {
                console.log('> Replicating data into Node 3!');
                if (req.crash_config.node3) { throw new Error('> Simulated Crash!'); }

                // Connection
                node3_connection = sql.createConnection(database_configs.node3);

                // Query
                if (!(values[2] < 1980)) {
                    console.log('> Swapping');
                    try {
                        // Connection
                        node2_connection = sql.createConnection(database_configs.node2);

                        // Query
                        let insert_statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                        await runExecuteLogs(node2_connection, insert_statement, values, [OPERATION.INSERT, STATUS.START, 2].concat(values), 'WRITE', 0);
                        let delete_statement = 'DELETE FROM movies WHERE id = ?';
                        await runExecuteLogs(node3_connection, delete_statement, [values[0]], [OPERATION.DELETE, STATUS.START, 3].concat(values), 'WRITE', 0);

                        node2_connection.end();
                        node3_connection.end();
                        console.log('> Update Movie SWAP Node 2 & 3 Success!');
                    } catch (err) {
                        console.log('> Node 2 is unavailable! [UPDATE-REP]');
                        if (node2_connection != null) { node2_connection.end(); }
                    }
                } else {
                    let statement = 'UPDATE movies SET `name` = ?, `year` = ?, `rank` = ? WHERE id = ?';
                    await runExecuteLogs(node3_connection, statement, [values[1], values[2], values[3], values[0]], [OPERATION.UPDATE, STATUS.START, 2].concat(values), 'WRITE', 3);
            
                    node3_connection.end();
                    console.log('> Update Movie Node 3 Success!');
                }    
            } catch (err) {
                console.log('> Node 3 is unavailable! [UPDATE-REP]');
                if (node3_connection != null) { node3_connection.end(); }
            } 
        } 
    },
    postDeleteMovie: async (req, res, next) => {
        var node1_connection;
        var node2_connection;
        var node3_connection;
        var values = [parseInt(req.body.id), null, parseInt(req.body.year), null];
        var replicateFlag = false;  

        try {
            console.log('> Deleting data from node 1');
            if (req.crash_config.node1) { throw new Error('> Simulated Crash!'); }

            // Connection
            node1_connection = sql.createConnection(database_configs.node1);

            // Query
            let statement = 'DELETE FROM movies WHERE id = ?';
            await runExecuteLogs(node1_connection, statement, [values[0]], [OPERATION.DELETE, STATUS.START, 1].concat(values), 'WRITE', 0);

            node1_connection.end();
            replicateFlag = true;
            console.log('> Delete Movie Node 1 Success!');
        } catch (err) {
            console.log('> Node 1 is unavailable!');
            if (node1_connection != null) { node1_connection.end(); }
        }

        // Replicate
        if (!replicateFlag) { return; }
        if (values[2] >= 1980) {                            // Node 2
            try {
                console.log('> Replicating data into Node 2!');
                if (req.crash_config.node2) { throw new Error('> Simulated Crash!'); }

                // Connection
                node2_connection = sql.createConnection(database_configs.node2);

                // Query
                let statement = 'DELETE FROM movies WHERE id = ?';
                await runExecuteLogs(node2_connection, statement, [values[0]], [OPERATION.DELETE, STATUS.START, 2].concat(values), 'WRITE', 0);

                node2_connection.end();
                console.log('> Delete Movie Node 2 Success!');
            } catch (err) {
                console.log('> Node 2 is unavailable! [INSERT-REP]');
                if (node2_connection != null) { node2_connection.end(); }
            }   
        } else {                                            // Node 3
            try {
                console.log('> Replicating data into Node 3!');
                if (req.crash_config.node3) { throw new Error('> Simulated Crash!'); }

                // Connection
                node3_connection = sql.createConnection(database_configs.node3);

                // Query
                let statement = 'DELETE FROM movies WHERE id = ?';
                await runExecuteLogs(node3_connection, statement, [values[0]], [OPERATION.DELETE, STATUS.START, 3].concat(values), 'WRITE', 0);

                node3_connection.end();
                console.log('> Delete Movie Node 3 Success!');
            } catch (err) {
                console.log('> Node 3 is unavailable! [INSERT-REP]');
                if (node3_connection != null) { node3_connection.end(); }
            }
        }     
    }
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
async function runExecuteLogs(connection, statement, values, log_values, type, id_index) {
    var logs_connection;
    
    try {
        // Connections (Log)
        logs_connection = sql.createConnection(database_configs.dblog);
        
        // Insert Logs
        let log_statement = 'INSERT INTO logs (`operation`, `status`, `node`, `id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?, ?, ?, ?)';
        await logs_connection.promise().execute(log_statement, log_values);

        // Set Transaction Timeout (10s)
        await connection.promise().query('SET innodb_lock_wait_timeout = 10');

        // Transaction w/ Logs
        await connection.promise().query('SET AUTOCOMMIT = 0');                     // Lock & Start Transaction
        await connection.promise().query('START TRANSACTION');
        await connection.promise().query(`LOCK TABLES movies ${type}`);
        await updateLogs(logs_connection, [STATUS.WRITE, values[id_index]]);
        await connection.promise().execute(statement, values);                      // SQL Operations
        await updateLogs(logs_connection, [STATUS.COMMITTING, values[id_index]]);
        await connection.promise().query('COMMIT');                                 // Commit
        await updateLogs(logs_connection, [STATUS.COMMITTED, values[id_index]]);
        await connection.promise().query('UNLOCK TABLES');                          // Unlock
        await updateLogs(logs_connection, [STATUS.SUCCESS, values[id_index]]);
    } catch (err) {
        console.log('> An error occured in a transaction!');
        console.log(err);

        // Error Logs
        await updateLogs(logs_connection, [STATUS.TERMINATE, values[id_index]]);  
        throw (err);         
    };
};

/** 
 *  Repetitive operations for prepared logging MySQL queries.
*/
async function updateLogs(connection, values) {
    console.log(values);
    let statement = `UPDATE logs SET status = ? WHERE id = ? AND status != ${STATUS.TERMINATE}`;
    try {
        await connection.promise().execute(statement, values);
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
