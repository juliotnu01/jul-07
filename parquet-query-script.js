// query-parquet.js
const parquet = require('parquetjs');
const path = require('path');

const MASTER_PARQUET = path.join(__dirname, 'binance-master-data', 'master', 'BTCUSDT-master.parquet');

async function queryParquet(limit = 100, options = {}) {
    try {
        console.log(`📖 Leyendo: ${MASTER_PARQUET}\n`);

        const reader = await parquet.ParquetReader.openFile(MASTER_PARQUET);
        const cursor = reader.getCursor();
        
        const totalRecords = reader.getRowCount();
        const schema = reader.getSchema();
        
        console.log(`📊 Total de registros: ${totalRecords.toLocaleString()}`);
        console.log(`📋 Columnas: ${Object.keys(schema.fields).join(', ')}\n`);

        const records = [];
        let record;
        let count = 0;

        // Opción para saltar registros (offset)
        if (options.offset) {
            console.log(`⏩ Saltando ${options.offset} registros...`);
            while (count < options.offset && (record = await cursor.next())) {
                count++;
            }
            count = 0;
        }

        // Leer los registros
        while ((record = await cursor.next()) && count < limit) {
            records.push(record);
            count++;
        }

        await reader.close();

        // Mostrar resultados
        if (options.format === 'json') {
            console.log(JSON.stringify(records, null, 2));
        } else if (options.format === 'csv') {
            printCSV(records);
        } else if (options.format === 'stats') {
            printStats(records);
        } else {
            printTable(records, options.columns);
        }

        return records;

    } catch (error) {
        console.error('❌ Error:', error.message);
        process.exit(1);
    }
}

function printTable(records, selectedColumns = null) {
    if (records.length === 0) {
        console.log('❌ No hay registros');
        return;
    }

    const columns = selectedColumns || Object.keys(records[0]);
    
    console.log('\n' + '='.repeat(120));
    console.log(`📋 Mostrando ${records.length} registros:\n`);

    // Encabezado
    const header = columns.map(c => c.padEnd(15)).join(' | ');
    console.log(header);
    console.log('-'.repeat(120));

    // Filas
    records.forEach((record, idx) => {
        const row = columns.map(col => {
            let value = record[col];
            
            // Formatear timestamp
            if (col === 'timestamp') {
                value = new Date(value).toISOString().slice(0, 19).replace('T', ' ');
            }
            // Formatear números
            else if (typeof value === 'number') {
                if (Number.isInteger(value)) {
                    value = value.toString();
                } else {
                    value = value.toFixed(4);
                }
            }
            // Manejar null/undefined
            else if (value === null || value === undefined) {
                value = 'null';
            }
            
            return String(value).padEnd(15).slice(0, 15);
        }).join(' | ');
        
        console.log(`${String(idx + 1).padStart(3)} | ${row}`);
    });
    
    console.log('='.repeat(120) + '\n');
}

function printCSV(records) {
    if (records.length === 0) return;
    
    const columns = Object.keys(records[0]);
    
    // Encabezado
    console.log(columns.join(','));
    
    // Filas
    records.forEach(record => {
        const row = columns.map(col => {
            let value = record[col];
            if (value === null || value === undefined) value = '';
            return value;
        });
        console.log(row.join(','));
    });
}

function printStats(records) {
    if (records.length === 0) return;

    console.log('\n📊 Estadísticas de los registros:\n');

    const numericColumns = Object.keys(records[0]).filter(col => 
        typeof records[0][col] === 'number' && col !== 'timestamp'
    );

    numericColumns.forEach(col => {
        const values = records.map(r => r[col]).filter(v => v !== null && v !== undefined);
        
        if (values.length === 0) return;

        const sum = values.reduce((a, b) => a + b, 0);
        const avg = sum / values.length;
        const min = Math.min(...values);
        const max = Math.max(...values);
        
        console.log(`${col.padEnd(25)}: min=${min.toFixed(4)} max=${max.toFixed(4)} avg=${avg.toFixed(4)}`);
    });

    console.log('');
}

// ========== CLI ==========

const args = process.argv.slice(2);
const options = {
    limit: 100,
    offset: 0,
    format: 'table',
    columns: null
};

// Parsear argumentos
for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
        case '-n':
        case '--limit':
            options.limit = parseInt(args[++i]);
            break;
        case '-o':
        case '--offset':
            options.offset = parseInt(args[++i]);
            break;
        case '-f':
        case '--format':
            options.format = args[++i]; // table, json, csv, stats
            break;
        case '-c':
        case '--columns':
            options.columns = args[++i].split(',');
            break;
        case '-h':
        case '--help':
            console.log(`
📖 Consulta de archivos Parquet

Uso: node query-parquet.js [opciones]

Opciones:
  -n, --limit <num>      Número de registros (default: 100)
  -o, --offset <num>     Saltar N registros (default: 0)
  -f, --format <tipo>    Formato: table|json|csv|stats (default: table)
  -c, --columns <cols>   Columnas a mostrar (separadas por coma)
  -h, --help             Mostrar ayuda

Ejemplos:
  node query-parquet.js
  node query-parquet.js -n 50
  node query-parquet.js -n 20 -o 1000
  node query-parquet.js -f json -n 10
  node query-parquet.js -f csv -n 100 > output.csv
  node query-parquet.js -f stats -n 1000
  node query-parquet.js -c timestamp,close,volume,rsi_14 -n 50
`);
            process.exit(0);
        default:
            console.error(`❌ Opción desconocida: ${args[i]}`);
            console.log('Usa -h para ver la ayuda');
            process.exit(1);
    }
}

// Ejecutar consulta
queryParquet(options.limit, options);