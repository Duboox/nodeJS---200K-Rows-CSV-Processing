const fs = require('fs');
const { Readable } = require("stream");
const csv = require("csv-parser");
//CRM
const { AsyncParser } = require('@json2csv/node')
const JSZip = require('jszip')

function cronJobExecution(url, chunkSize = 25000) {
  // 8.000.000 / 25.000 = 320
  return new Promise(async (resolve, reject) => {
    const readableStream = await fetch(url).then((r) =>
      Readable.fromWeb(r.body)
    );
    let processedRecords = 0;
    let chunks = [];
    let wholeData = [];
    readableStream
      .pipe(csv({ delimiter: "," }))
      .on("data", (record) => {
        chunks.push(record);
        wholeData.push(record);
        processedRecords++;

        if (processedRecords % chunkSize === 0) {
          uploadToZohoCRM(chunks);
          chunks = [];
        }
      })
      .on("end", async () => {
        //Validation if chunk Size is greater than file rows
        if (chunks.length > 0) {;
          uploadToZohoCRM(chunks);
        }
        uploadWholeDataToS3(wholeData);
        resolve();
      })
      .on("error", (err) => reject(err));
  });
}

async function uploadToZohoCRM(dataJSON) {
  //CALL To Another Function in Catalyst
  const parser = new AsyncParser()
  const zip = new JSZip()
  const csv = await parser.parse(JSON.stringify(dataJSON)).promise()
  zip.file('upload.csv', csv)
  const zipContent = await zip.generateAsync({ type: 'nodebuffer' })
  const formData = new FormData()
  const blob = new Blob([zipContent], { type: 'application/zip' });
  formData.append('file', blob, 'upload.zip')

  //Make Request to zoho CRM Uploading the file
  console.timeEnd("testing");
  setTimeout(() => {
    //Check content Length of FormData
    const resultContentLength = Array.from(formData.entries(), ([key, prop]) => (
      {[key]: {
        "ContentLength": 
        typeof prop === "string" 
        ? prop.length 
        : prop.size
      }
    }));
  
  console.log('UPLOADED ZIP WITH CSV TO CRM FormData', resultContentLength);
  }, 5000)
}

async function uploadWholeDataToS3(data) {
  //CALL To Another Function in Catalyst
  const jsonData = JSON.stringify(data, null);
  const buffer = Buffer.from(jsonData, 'utf8'); // You can upload buffer to s3 directly
  fs.writeFile('data.json', buffer, (err) => {
    if (err) {
      console.error('Error writing file:', err);
    } else {
      console.log('JSON buffer file created successfully!');
    }
  });
}

// 200.000 ROWS
console.time("testing");
const result = cronJobExecution(
  "https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/BoPIIP-December-2023-quarter/Download-data/balance-of-payments-and-international-investment-position-december-2023-quarter.csv"
)
// 17.000 Rows
// const result = cronJobExecution(
//   "https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-size-bands-csv.csv"
// )
  .then(() => console.log("Proceso finalizado correctamente."))
  .catch((err) => console.error(err));
