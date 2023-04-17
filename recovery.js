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

var node1_connection;
var node2_connection;
var node3_connection;
var logs_connection;
var interval = 20 * 1000;

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
};

async function recovery() {
    while (true) {
        //  Node 1 Recovery
        try {
            console.log('> Fetching Logs [Node 1]');
            // Connections
            logs_connection = sql.createConnection(database_configs.dblog);
            node1_connection = sql.createConnection(database_configs.node1);

            // Fetch Logs (20 Logs Maximum)
            [rows, fields] = await logs_connection.promise().query(`SELECT * FROM logs WHERE status = ${STATUS.TERMINATE} AND node = 1 LIMIT 20`);

            rows.forEach(row => {
                if (row.operation == OPERATION.INSERT) {
                    let statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node1_connection.execute(statement, [row.id, row.name, row.year, row.rank]);
                    logs_connection.execute(log_statement, [STATUS.SUCCESS, OPERATION.INSERT, STATUS.TERMINATE, 1, row.id]);
                    console.log('> Inserted movie data on Node 1! [RECOVERY]');
                } 

                if (row.operation == OPERATION.DELETE) {
                    let statement = 'DELETE FROM movies WHERE id = ?';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node1_connection.execute(statement, [row.id]);
                    logs_connection.execute(log_statement, [STATUS.SUCCESS, OPERATION.DELETE, STATUS.TERMINATE, 1, row.id]);
                    console.log('> Deleted movie data on Node 1! [RECOVERY]');
                } 

                if (row.operation == OPERATION.UPDATE) {
                    let statement = 'UPDATE movies SET `name` = ?, `year` = ?, `rank` = ? WHERE id = ?';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node1_connection.execute(statement, [row.name, row.year. row.rank, row.id]);
                    logs_connection.execute(log_statement, [STATUS.SUCCESS, OPERATION.UPDATE, STATUS.TERMINATE, 1, row.id]);
                    console.log('> Updated movie data on Node 1! [RECOVERY]');
                } 
            });

            node1_connection.end();
            logs_connection.end();          
        } catch (err) {
            console.log('> Node 1 is Unavailable!');
            console.log(err);
            if (node1_connection != null) { node1_connection.end(); }    
            if (logs_connection != null) { logs_connection.end(); }    
        }

        //  Node 2 Recovery
        try {
            console.log('> Fetching Logs [Node 2]');
            // Connections
            logs_connection = sql.createConnection(database_configs.dblog);
            node2_connection = sql.createConnection(database_configs.node2);

            // Fetch Logs (20 Logs Maximum)
            [rows, fields] = await logs_connection.promise().query(`SELECT * FROM logs WHERE status = ${STATUS.TERMINATE} AND node = 2 LIMIT 20`);

            rows.forEach(row => {
                if (row.operation == OPERATION.INSERT) {
                    let statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node2_connection.execute(statement, [row.id, row.name, row.year, row.rank]);
                    logs_connection.promise().execute(log_statement, [STATUS.SUCCESS, OPERATION.INSERT, STATUS.TERMINATE, 2, row.id]);
                    console.log('> Inserted movie data on Node 1! [RECOVERY]');
                } 

                if (row.operation == OPERATION.DELETE) {
                    let statement = 'DELETE FROM movies WHERE id = ?';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node2_connection.execute(statement, [row.id]);
                    logs_connection.promise().execute(log_statement, [STATUS.SUCCESS, OPERATION.DELETE, STATUS.TERMINATE, 2, row.id]);
                    console.log('> Deleted movie data on Node 1! [RECOVERY]');
                } 

                if (row.operation == OPERATION.UPDATE) {
                    let statement = 'UPDATE movies SET `name` = ?, `year` = ?, `rank` = ? WHERE id = ?';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node2_connection.execute(statement, [row.name, row.year, row.rank, row.id]);
                    logs_connection.promise().execute(log_statement, [STATUS.SUCCESS, OPERATION.UPDATE, STATUS.TERMINATE, 2, row.id]);
                    console.log('> Updated movie data on Node 1! [RECOVERY]');
                } 
            });

            node2_connection.end();
            logs_connection.end();          
        } catch (err) {
            console.log('> Node 2 is Unavailable!');
            console.log(err);
            if (node2_connection != null) { node2_connection.end(); }    
            if (logs_connection != null) { logs_connection.end(); }    
        }

        //  Node 3 Recovery
        try {
            console.log('> Fetching Logs [Node 3]');
            // Connections
            logs_connection = sql.createConnection(database_configs.dblog);
            node3_connection = sql.createConnection(database_configs.node3);

            // Fetch Logs (20 Logs Maximum)
            [rows, fields] = await logs_connection.promise().query(`SELECT * FROM logs WHERE status = ${STATUS.TERMINATE} AND node = 3 LIMIT 20`);

            rows.forEach(row => {
                if (row.operation == OPERATION.INSERT) {
                    let statement = 'INSERT INTO movies (`id`, `name`, `year`, `rank`) VALUES (?, ?, ?, ?)';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node3_connection.execute(statement, [row.id, row.name, row.year, row.rank]);
                    logs_connection.execute(log_statement, [STATUS.SUCCESS, OPERATION.INSERT, STATUS.TERMINATE, 3, row.id]);
                    console.log('> Inserted movie data on Node 1! [RECOVERY]');
                } 

                if (row.operation == OPERATION.DELETE) {
                    let statement = 'DELETE FROM movies WHERE id = ?';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node3_connection.execute(statement, [row.id]);
                    logs_connection.execute(log_statement, [STATUS.SUCCESS, OPERATION.DELETE, STATUS.TERMINATE, 3, row.id]);
                    console.log('> Deleted movie data on Node 1! [RECOVERY]');
                } 

                if (row.operation == OPERATION.UPDATE) {
                    let statement = 'UPDATE movies SET `name` = ?, `year` = ?, `rank` = ? WHERE id = ?';
                    let log_statement = 'UPDATE logs SET status = ? WHERE operation = ? AND status = ? AND node = ? AND id = ?';
                    node3_connection.execute(statement, [row.name, row.year. row.rank, row.id]);
                    logs_connection.execute(log_statement, [STATUS.SUCCESS, OPERATION.UPDATE, STATUS.TERMINATE, 3, row.id]);
                    console.log('> Updated movie data on Node 1! [RECOVERY]');
                } 
            });

            node3_connection.end();
            logs_connection.end();          
        } catch (err) {
            console.log('> Node 3 is Unavailable!');
            console.log(err);
            if (node3_connection != null) { node3_connection.end(); }    
            if (logs_connection != null) { logs_connection.end(); }    
        }
        
        console.log('----- ----- ----- ----- ---- | ' + new Date());
        await sleep(interval); 
    }
};

console.log('> Recovery Monitor is online!');
recovery();