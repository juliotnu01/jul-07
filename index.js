const express = require('express');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const AdmZip = require('adm-zip');
const parquet = require('parquetjs');

const app = express();
const PORT = process.env.PORT || 3000;

// ========== CONFIGURACIÓN ==========
const BASE_DIR = path.join(__dirname, 'binance-master-data');
const DIRECTORIES = {
    klines: path.join(BASE_DIR, 'klines'),
    metrics: path.join(BASE_DIR, 'metrics'),
    master: path.join(BASE_DIR, 'master')
};

const MASTER_PARQUET = path.join(DIRECTORIES.master, 'BTCUSDT-master.parquet');

const BASE_URLS = {
    klines: 'https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m',
    metrics: 'https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT'
};

// Crear directorios
Object.values(DIRECTORIES).forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// ========== INDICADORES ==========

class RSICalculator {
    constructor(period = 14) {
        this.period = period;
        this.prices = [];
        this.avgGain = null;
        this.avgLoss = null;
        this.isInitialized = false;
    }

    calculate(close) {
        this.prices.push(close);

        if (this.prices.length < this.period + 1) return null;

        const changes = [];
        for (let i = 1; i < this.prices.length; i++) {
            changes.push(this.prices[i] - this.prices[i - 1]);
        }

        if (!this.isInitialized) {
            let sumGain = 0, sumLoss = 0;
            for (let i = 0; i < this.period; i++) {
                if (changes[i] > 0) sumGain += changes[i];
                else sumLoss += Math.abs(changes[i]);
            }
            this.avgGain = sumGain / this.period;
            this.avgLoss = sumLoss / this.period;
            this.isInitialized = true;

            if (this.prices.length > this.period + 1) {
                this.prices = this.prices.slice(-this.period - 1);
            }
        } else {
            const lastChange = changes[changes.length - 1];
            const gain = lastChange > 0 ? lastChange : 0;
            const loss = lastChange < 0 ? Math.abs(lastChange) : 0;

            this.avgGain = (this.avgGain * (this.period - 1) + gain) / this.period;
            this.avgLoss = (this.avgLoss * (this.period - 1) + loss) / this.period;

            if (this.prices.length > this.period + 1) this.prices.shift();
        }

        if (this.avgLoss === 0) return this.avgGain === 0 ? 50 : 100;
        const rs = this.avgGain / this.avgLoss;
        return 100 - (100 / (1 + rs));
    }
}

class CVDCalculator {
    constructor() {
        this.cvd = 0;
    }

    calculate(takerBuyVol, totalVol) {
        const takerSellVol = totalVol - takerBuyVol;
        const delta = takerBuyVol - takerSellVol;
        this.cvd += delta;
        return { cvd: this.cvd, delta };
    }
}



// ========== DESCARGA ==========

async function downloadFile(url, filePath) {
    try {
        console.log(`📥 ${path.basename(filePath)}`);
        const response = await axios({
            method: 'GET',
            url: url,
            responseType: 'stream',
            timeout: 300000,
            maxRedirects: 5
        });

        const writer = fs.createWriteStream(filePath);
        response.data.pipe(writer);

        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
        });
    } catch (error) {
        if (error.response?.status === 404) throw new Error('404');
        throw error;
    }
}

function getDateRange(yearsBack = 2) {
    const dates = [];
    const end = new Date();
    const start = new Date(end);
    start.setFullYear(start.getFullYear() - yearsBack);

    const current = new Date(start);
    while (current <= end) {
        const y = current.getFullYear();
        const m = String(current.getMonth() + 1).padStart(2, '0');
        const d = String(current.getDate()).padStart(2, '0');
        dates.push(`${y}-${m}-${d}`);
        current.setDate(current.getDate() + 1);
    }
    return dates;
}

async function downloadDateBatch(date) {
    const tasks = [];

    const klineFile = `BTCUSDT-1m-${date}.zip`;
    const klinePath = path.join(DIRECTORIES.klines, klineFile);
    if (!fs.existsSync(klinePath)) {
        tasks.push(
            downloadFile(`${BASE_URLS.klines}/${klineFile}`, klinePath)
                .then(() => ({ type: 'klines', status: 'ok' }))
                .catch(() => ({ type: 'klines', status: 'fail' }))
        );
    }

    const metricFile = `BTCUSDT-metrics-${date}.zip`;
    const metricPath = path.join(DIRECTORIES.metrics, metricFile);
    if (!fs.existsSync(metricPath)) {
        tasks.push(
            downloadFile(`${BASE_URLS.metrics}/${metricFile}`, metricPath)
                .then(() => ({ type: 'metrics', status: 'ok' }))
                .catch(() => ({ type: 'metrics', status: 'fail' }))
        );
    }

    return tasks.length > 0 ? Promise.all(tasks) : [];
}

async function downloadAllData() {
    try {
        const dates = getDateRange(2);
        const stats = { klines: 0, metrics: 0 };

        console.log(`📅 Descargando ${dates.length} días en lotes de 5...`);

        for (let i = 0; i < dates.length; i += 5) {
            const batch = dates.slice(i, i + 5);
            const results = await Promise.all(batch.map(downloadDateBatch));
            
            results.flat().forEach(r => {
                if (r?.status === 'ok') stats[r.type]++;
            });

            console.log(`✅ Lote ${Math.floor(i / 5) + 1}/${Math.ceil(dates.length / 5)}`);
            await new Promise(r => setTimeout(r, 1000));
        }

        console.log(`\n📊 Klines: ${stats.klines} | Metrics: ${stats.metrics}`);
        return stats;
    } catch (error) {
        console.error('❌ Error en descarga:', error.message);
        throw error;
    }
}

// ========== PROCESAMIENTO ==========

function processKlines(csvData) {
    const lines = csvData.trim().split('\n').slice(1);
    return lines.filter(l => l.trim()).map(line => {
        const v = line.split(',');
        return {
            ts: parseInt(v[0]),
            o: parseFloat(v[1]),
            h: parseFloat(v[2]),
            l: parseFloat(v[3]),
            c: parseFloat(v[4]),
            v: parseFloat(v[5]),
            ct: parseInt(v[6]),
            qv: parseFloat(v[7]),
            n: parseInt(v[8]),
            tbv: parseFloat(v[9]),
            tbqv: parseFloat(v[10])
        };
    });
}

function processMetrics(csvData) {
    const lines = csvData.trim().split('\n').slice(1);
    const map = new Map();
    
    lines.filter(l => l.trim()).forEach(line => {
        const v = line.split(',');
        // create_time está en formato "2023-11-15 00:00:00"
        const dateStr = v[0].trim();
        const date = new Date(dateStr + ' UTC'); // Importante: especificar UTC
        const ts = date.getTime();
        
        map.set(ts, {
            oi: parseFloat(v[2]),      // sum_open_interest
            oiv: parseFloat(v[3])      // sum_open_interest_value
        });
    });
    
    console.log(`   📊 Metrics procesados: ${map.size} registros`);
    if (map.size > 0) {
        const firstKey = Array.from(map.keys())[0];
        const firstDate = new Date(firstKey);
        console.log(`   🕐 Primer metric: ${firstDate.toISOString()}`);
    }
    
    return map;
}

function extractZip(zipPath, processor) {
    if (!fs.existsSync(zipPath)) return null;
    
    try {
        const zip = new AdmZip(zipPath);
        const entry = zip.getEntries().find(e => e.name.endsWith('.csv'));
        if (entry) {
            return processor(zip.readAsText(entry));
        }
    } catch (e) {
        console.error(`❌ Error en ${path.basename(zipPath)}: ${e.message}`);
    }
    return null;
}

// ========== CONSTRUCCIÓN MASTER ==========

async function buildMasterDataFrame() {
    console.log('🏗️  Construyendo Master DataFrame...\n');

    try {
        // Verificar que existan los directorios
        if (!fs.existsSync(DIRECTORIES.klines)) {
            throw new Error(`Directorio no existe: ${DIRECTORIES.klines}`);
        }
        if (!fs.existsSync(DIRECTORIES.metrics)) {
            throw new Error(`Directorio no existe: ${DIRECTORIES.metrics}`);
        }

        // Obtener TODOS los archivos ZIP que existen
        const klineFiles = fs.readdirSync(DIRECTORIES.klines)
            .filter(f => f.endsWith('.zip'))
            .sort();
        
        const metricFiles = fs.readdirSync(DIRECTORIES.metrics)
            .filter(f => f.endsWith('.zip'))
            .sort();

        console.log(`📂 Archivos encontrados:`);
        console.log(`   Klines: ${klineFiles.length} archivos`);
        console.log(`   Metrics: ${metricFiles.length} archivos`);

        if (klineFiles.length === 0) {
            throw new Error('No hay archivos klines. Verifica la carpeta: ' + DIRECTORIES.klines);
        }

        let totalRecords = 0;

        // Recolectar klines
        console.log('\n📦 Procesando klines...');
        let allKlines = [];
        let processedFiles = 0;
        
        for (const file of klineFiles) {
            try {
                const klinesPath = path.join(DIRECTORIES.klines, file);
                const klines = extractZip(klinesPath, processKlines);
                if (klines && klines.length > 0) {
                    allKlines = allKlines.concat(klines);
                    processedFiles++;
                    if (processedFiles % 50 === 0) {
                        console.log(`   📊 ${processedFiles}/${klineFiles.length} archivos procesados...`);
                    }
                }
            } catch (error) {
                console.error(`❌ Error procesando ${file}:`, error.message);
            }
        }

        if (allKlines.length === 0) {
            throw new Error('No se encontraron klines. Ejecuta /download-all primero');
        }

        allKlines.sort((a, b) => a.ts - b.ts);
        console.log(`✅ Total klines: ${allKlines.length.toLocaleString()}`);

        // Recolectar metrics
        console.log('\n📊 Procesando metrics...');
        const allMetricsMap = new Map();
        processedFiles = 0;
        
        for (const file of metricFiles) {
            try {
                const metricsPath = path.join(DIRECTORIES.metrics, file);
                const metricsMap = extractZip(metricsPath, processMetrics);
                if (metricsMap) {
                    metricsMap.forEach((value, key) => {
                        allMetricsMap.set(key, value);
                    });
                    processedFiles++;
                    if (processedFiles % 50 === 0) {
                        console.log(`   📊 ${processedFiles}/${metricFiles.length} archivos procesados...`);
                    }
                }
            } catch (error) {
                console.error(`❌ Error procesando metrics ${file}:`, error.message);
            }
        }
        
        console.log(`✅ Total metrics: ${allMetricsMap.size.toLocaleString()}`);

        // Inicializar calculadores
        const rsi = new RSICalculator(14);
        const cvd = new CVDCalculator();

        // Schema completo
        const schema = new parquet.ParquetSchema({
            timestamp: { type: 'INT64' },
            timestamp_utc: { type: 'UTF8' },
            timestamp_venezuela: { type: 'UTF8' },
            open: { type: 'FLOAT' },
            high: { type: 'FLOAT' },
            low: { type: 'FLOAT' },
            close: { type: 'FLOAT' },
            volume: { type: 'FLOAT' },
            quote_volume: { type: 'FLOAT' },
            trades_count: { type: 'INT32' },
            taker_buy_vol: { type: 'FLOAT' },
            taker_buy_quote_vol: { type: 'FLOAT' },
            open_interest: { type: 'FLOAT', optional: true },
            open_interest_value: { type: 'FLOAT', optional: true },
            rsi_14: { type: 'FLOAT', optional: true },
            cvd: { type: 'FLOAT' },
            cvd_delta: { type: 'FLOAT' }
        });

        const writer = await parquet.ParquetWriter.openFile(schema, MASTER_PARQUET, {
            compression: 'GZIP',
            useDataPageV2: true
        });

        console.log('💾 Escribiendo datos...\n');
        let lastMetrics = null;
        let progressCount = 0;
        let metricsMatched = 0;
        let metricsForwardFilled = 0;

        for (const k of allKlines) {
            try {
                // Buscar metrics exactos para este timestamp
                if (allMetricsMap.has(k.ts)) {
                    lastMetrics = allMetricsMap.get(k.ts);
                    metricsMatched++;
                } else if (lastMetrics) {
                    // Forward-fill: usar el último valor conocido
                    metricsForwardFilled++;
                }

                // Calcular indicadores
                const rsiVal = rsi.calculate(k.c);
                const cvdData = cvd.calculate(k.tbv, k.v);

                // Formatear timestamps
                const dateUTC = new Date(k.ts);
                const dateVET = new Date(k.ts - (4 * 60 * 60 * 1000));
                
                const timestamp_utc = dateUTC.toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
                const timestamp_venezuela = dateVET.toISOString().replace('T', ' ').slice(0, 19) + ' VET';

                await writer.appendRow({
                    timestamp: k.ts,
                    timestamp_utc: timestamp_utc,
                    timestamp_venezuela: timestamp_venezuela,
                    open: k.o,
                    high: k.h,
                    low: k.l,
                    close: k.c,
                    volume: k.v,
                    quote_volume: k.qv,
                    trades_count: k.n,
                    taker_buy_vol: k.tbv,
                    taker_buy_quote_vol: k.tbqv,
                    open_interest: lastMetrics?.oi,
                    open_interest_value: lastMetrics?.oiv,
                    rsi_14: rsiVal,
                    cvd: cvdData.cvd,
                    cvd_delta: cvdData.delta
                });

                totalRecords++;
                progressCount++;

                if (progressCount % 100000 === 0) {
                    console.log(`   📈 ${totalRecords.toLocaleString()} registros procesados...`);
                }
            } catch (error) {
                console.error(`❌ Error en timestamp ${k.ts}:`, error.message);
            }
        }

        await writer.close();

        const stats = fs.statSync(MASTER_PARQUET);
        console.log(`\n✅ COMPLETADO:`);
        console.log(`   📈 Registros: ${totalRecords.toLocaleString()}`);
        console.log(`   💾 Tamaño: ${(stats.size / (1024 * 1024)).toFixed(2)} MB`);
        console.log(`   📁 ${MASTER_PARQUET}`);
        console.log(`\n📊 Open Interest:`);
        console.log(`   ✅ Coincidencias exactas: ${metricsMatched.toLocaleString()}`);
        console.log(`   🔄 Forward-filled: ${metricsForwardFilled.toLocaleString()}`);
        console.log(`   ⚠️  Sin datos: ${(totalRecords - metricsMatched - metricsForwardFilled).toLocaleString()}`);

        return {
            success: true,
            records: totalRecords,
            sizeMB: (stats.size / (1024 * 1024)).toFixed(2),
            file: MASTER_PARQUET,
            oi_stats: {
                matched: metricsMatched,
                forward_filled: metricsForwardFilled,
                null_records: totalRecords - metricsMatched - metricsForwardFilled
            }
        };

    } catch (error) {
        console.error('💥 ERROR CRÍTICO:', error.message);
        console.error(error.stack);
        throw error;
    }
}

// ========== ENDPOINTS ==========

app.get('/download-all', async (req, res) => {
    try {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.write('🚀 Iniciando descarga...\n\n');
        
        const results = await downloadAllData();
        
        res.write('\n✅ Descarga completada\n');
        res.write(`📊 Klines: ${results.klines}\n`);
        res.write(`📈 Metrics: ${results.metrics}\n`);
        res.end();
    } catch (error) {
        console.error('Error en /download-all:', error);
        res.status(500).send(`❌ Error: ${error.message}`);
    }
});

app.get('/build-master', async (req, res) => {
    try {
        const result = await buildMasterDataFrame();
        res.json(result);
    } catch (error) {
        console.error('Error en /build-master:', error);
        res.status(500).json({ 
            error: error.message,
            stack: error.stack 
        });
    }
});

app.get('/master-preview', async (req, res) => {
    try {
        if (!fs.existsSync(MASTER_PARQUET)) {
            return res.status(404).json({ error: 'Master no encontrado' });
        }

        const reader = await parquet.ParquetReader.openFile(MASTER_PARQUET);
        const cursor = reader.getCursor();
        const preview = [];
        
        let record;
        let count = 0;
        while ((record = await cursor.next()) && count < 20) {
            preview.push(record);
            count++;
        }

        const stats = fs.statSync(MASTER_PARQUET);
        
        await reader.close();

        res.json({
            totalRecords: reader.getRowCount(),
            sizeMB: (stats.size / (1024 * 1024)).toFixed(2),
            preview: preview,
            schema: Object.keys(preview[0] || {}),
            note: "timestamp_utc = hora Binance | timestamp_venezuela = hora local VET (UTC-4)"
        });
    } catch (error) {
        console.error('Error en /master-preview:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/status', (req, res) => {
    try {
        const status = {};

        Object.entries(DIRECTORIES).forEach(([key, dir]) => {
            if (key !== 'master' && fs.existsSync(dir)) {
                const files = fs.readdirSync(dir).filter(f => f.endsWith('.zip'));
                status[key] = files.length;
            }
        });

        if (fs.existsSync(MASTER_PARQUET)) {
            const stats = fs.statSync(MASTER_PARQUET);
            status.master = {
                exists: true,
                sizeMB: (stats.size / (1024 * 1024)).toFixed(2)
            };
        } else {
            status.master = { exists: false };
        }

        res.json(status);
    } catch (error) {
        console.error('Error en /status:', error);
        res.status(500).json({ error: error.message });
    }
});

// Error handler global
app.use((err, req, res, next) => {
    console.error('Error no capturado:', err);
    res.status(500).json({ 
        error: 'Error interno del servidor',
        message: err.message 
    });
});

// ========== SERVIDOR ==========

app.listen(PORT, () => {
    console.log(`\n🚀 Servidor: http://localhost:${PORT}`);
    console.log('\n📋 ENDPOINTS:');
    console.log('   GET /download-all   - Descargar datos');
    console.log('   GET /build-master   - Construir dataset');
    console.log('   GET /master-preview - Ver primeros registros');
    console.log('   GET /status         - Estado actual');
});