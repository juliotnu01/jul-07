// verify-data.js
// Script para verificar datos contra Binance API en tiempo real

const axios = require('axios');
const parquet = require('parquetjs');
const path = require('path');

const MASTER_PARQUET = path.join(__dirname, 'binance-master-data', 'master', 'BTCUSDT-master.parquet');

// Función para obtener datos de Binance API
async function getBinanceKline(timestamp) {
    try {
        // Binance API para klines
        const url = 'https://fapi.binance.com/fapi/v1/klines';
        const params = {
            symbol: 'BTCUSDT',
            interval: '1m',
            startTime: timestamp,
            endTime: timestamp,
            limit: 1
        };

        const response = await axios.get(url, { params });
        
        if (response.data && response.data.length > 0) {
            const kline = response.data[0];
            return {
                timestamp: parseInt(kline[0]),
                open: parseFloat(kline[1]),
                high: parseFloat(kline[2]),
                low: parseFloat(kline[3]),
                close: parseFloat(kline[4]),
                volume: parseFloat(kline[5]),
                close_time: parseInt(kline[6]),
                quote_volume: parseFloat(kline[7]),
                count: parseInt(kline[8]),
                taker_buy_volume: parseFloat(kline[9]),
                taker_buy_quote_volume: parseFloat(kline[10])
            };
        }
        return null;
    } catch (error) {
        console.error('❌ Error consultando Binance API:', error.message);
        return null;
    }
}

// Función para leer un registro específico del parquet
async function getParquetRecord(timestamp) {
    try {
        const reader = await parquet.ParquetReader.openFile(MASTER_PARQUET);
        const cursor = reader.getCursor();
        
        let record;
        while ((record = await cursor.next())) {
            if (record.timestamp === timestamp) {
                await reader.close();
                return record;
            }
        }
        
        await reader.close();
        return null;
    } catch (error) {
        console.error('❌ Error leyendo parquet:', error.message);
        return null;
    }
}

// Función para comparar datos
function compareData(parquetData, binanceData) {
    console.log('\n' + '='.repeat(80));
    console.log('📊 COMPARACIÓN DE DATOS');
    console.log('='.repeat(80));
    
    const timestamp = parquetData.timestamp;
    const dateUTC = new Date(timestamp).toISOString();
    const dateLocal = new Date(timestamp).toString();
    
    console.log(`\n🕐 Timestamp: ${timestamp}`);
    console.log(`   UTC:   ${dateUTC}`);
    console.log(`   Local: ${dateLocal}`);
    
    console.log('\n📈 COMPARACIÓN DE VALORES:');
    console.log('-'.repeat(80));
    console.log('Campo                    | Parquet          | Binance API      | Diferencia');
    console.log('-'.repeat(80));
    
    const fields = ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume'];
    
    let allMatch = true;
    
    fields.forEach(field => {
        const parquetVal = parquetData[field];
        const binanceVal = binanceData[field];
        
        let match = false;
        let diff = 0;
        
        if (typeof parquetVal === 'number' && typeof binanceVal === 'number') {
            // Para precios, tolerancia de 0.01
            // Para volúmenes, tolerancia de 0.001
            const tolerance = field.includes('volume') || field === 'count' ? 0.001 : 0.01;
            diff = Math.abs(parquetVal - binanceVal);
            match = diff < tolerance;
            
            if (!match) allMatch = false;
            
            const matchSymbol = match ? '✅' : '❌';
            const pVal = parquetVal.toFixed(field === 'count' ? 0 : 8).padEnd(16);
            const bVal = binanceVal.toFixed(field === 'count' ? 0 : 8).padEnd(16);
            const dVal = diff.toFixed(8);
            
            console.log(`${field.padEnd(24)} | ${pVal} | ${bVal} | ${dVal} ${matchSymbol}`);
        }
    });
    
    console.log('-'.repeat(80));
    
    if (allMatch) {
        console.log('\n✅ ¡TODOS LOS DATOS COINCIDEN!');
    } else {
        console.log('\n⚠️  Hay diferencias en los datos');
        console.log('\n💡 Posibles causas:');
        console.log('   1. Datos descargados de fecha diferente (archivos históricos vs API actual)');
        console.log('   2. Binance actualiza datos históricos ocasionalmente');
        console.log('   3. Diferencias de redondeo en la conversión');
    }
    
    console.log('\n' + '='.repeat(80));
}

// Función principal
async function verifyTimestamp(timestamp) {
    console.log('\n🔍 Verificando datos contra Binance API...\n');
    
    // Validar que el timestamp sea válido
    if (!timestamp || isNaN(timestamp)) {
        console.error('❌ Timestamp inválido');
        return;
    }
    
    console.log(`📅 Consultando timestamp: ${timestamp}`);
    console.log(`   Fecha UTC: ${new Date(timestamp).toISOString()}`);
    console.log(`   Fecha Local: ${new Date(timestamp).toString()}\n`);
    
    // Obtener datos de Binance
    console.log('🌐 Consultando Binance API...');
    const binanceData = await getBinanceKline(timestamp);
    
    if (!binanceData) {
        console.error('❌ No se pudo obtener datos de Binance API');
        console.log('💡 Nota: La API solo tiene datos recientes (últimos meses)');
        console.log('   Para datos históricos, usa los archivos descargados de data.binance.vision');
        return;
    }
    
    console.log('✅ Datos obtenidos de Binance API\n');
    
    // Obtener datos del parquet
    console.log('📂 Buscando en archivo Parquet...');
    const parquetData = await getParquetRecord(timestamp);
    
    if (!parquetData) {
        console.error('❌ No se encontró el timestamp en el archivo Parquet');
        console.log('💡 Asegúrate de que el archivo master.parquet esté construido');
        return;
    }
    
    console.log('✅ Datos encontrados en Parquet\n');
    
    // Comparar
    compareData(parquetData, binanceData);
}

// Función para verificar fechas recientes (últimas 24h)
async function verifyRecent() {
    console.log('\n🔍 Verificando datos recientes (última hora)...\n');
    
    const now = Date.now();
    const oneHourAgo = now - (60 * 60 * 1000);
    
    // Redondear al minuto
    const timestamp = Math.floor(oneHourAgo / 60000) * 60000;
    
    await verifyTimestamp(timestamp);
}

// Función para mostrar información de un timestamp
function explainTimestamp(timestamp) {
    console.log('\n' + '='.repeat(80));
    console.log('📅 INFORMACIÓN DEL TIMESTAMP');
    console.log('='.repeat(80));
    console.log(`\nTimestamp: ${timestamp}`);
    console.log(`\nFecha UTC (Binance usa esto):`);
    console.log(`   ${new Date(timestamp).toUTCString()}`);
    console.log(`   ${new Date(timestamp).toISOString()}`);
    console.log(`\nFecha en tu zona horaria local:`);
    console.log(`   ${new Date(timestamp).toString()}`);
    console.log(`\nDía de la semana: ${new Date(timestamp).toLocaleDateString('es-ES', { weekday: 'long' })}`);
    console.log('\n' + '='.repeat(80));
    console.log('\n💡 IMPORTANTE:');
    console.log('   - Binance usa UTC (GMT+0) para todos los timestamps');
    console.log('   - Debes buscar la vela en Binance usando la hora UTC');
    console.log('   - En TradingView, asegúrate de tener la zona horaria en UTC');
    console.log('\n' + '='.repeat(80) + '\n');
}

// CLI
const args = process.argv.slice(2);

if (args.length === 0 || args[0] === '-h' || args[0] === '--help') {
    console.log(`
📊 Verificador de Datos Binance

Uso:
  node verify-data.js <timestamp>        Verificar un timestamp específico
  node verify-data.js --recent           Verificar datos recientes (última hora)
  node verify-data.js --explain <ts>     Explicar un timestamp (conversión de fechas)

Ejemplos:
  node verify-data.js 1700006400000
  node verify-data.js --recent
  node verify-data.js --explain 1700006400000

Nota: La API de Binance solo tiene datos recientes (últimos meses).
      Para verificar datos más antiguos, compara manualmente con los archivos
      descargados de https://data.binance.vision
`);
    process.exit(0);
}

// Ejecutar según el comando
if (args[0] === '--recent') {
    verifyRecent();
} else if (args[0] === '--explain') {
    const timestamp = parseInt(args[1]);
    if (isNaN(timestamp)) {
        console.error('❌ Timestamp inválido');
        process.exit(1);
    }
    explainTimestamp(timestamp);
} else {
    const timestamp = parseInt(args[0]);
    if (isNaN(timestamp)) {
        console.error('❌ Timestamp inválido');
        process.exit(1);
    }
    verifyTimestamp(timestamp);
}