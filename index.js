/*
  * This script will read a CSV file from S3, convert it to DynamoDB JSON format, and upload it to S3.
  * The user will then need to manually import the DynamoDB JSON file in S3 to DynamoDB
  * This script is meant to be run once to populate a new table in DynamoDB.
  * This should only be used when the dataset is too large to upload using the DynamoDB API.
  * 
  * If your dataset is small (say, less than 1000 items), you can use the DynamoDB API to upload the data.
  * See the following link for more information: https://github.com/robhogan/dynamodb-geo.js/blob/master/example/index.js
*/
const csv = require('csvtojson');
const { S3 } = require('@aws-sdk/client-s3');
const { marshall } = require('@aws-sdk/util-dynamodb');
const uuid = require('uuid');
const fs = require('fs');

const { generateGeoHash, generateHashKey } = require('./util');

const SOURCE_BUCKET = '';
const SOURCE_KEY = '';
const DESTINATION_BUCKET = '';
const DESTINATION_KEY = '';
const HASH_KEY_LENGTH = 4;
const LOCAL_FILE_PATH = '';
const CREATE_LOCAL_FILE = false;

const params = { Bucket: SOURCE_BUCKET, Key: SOURCE_KEY };
const config = { region: 'us-west-2' };
const s3 = new S3(config);

const main = async () => {
  try {
    console.log('Reading CSV file from S3...');
    const { Body: file } = await s3.getObject(params);
    const csvData = await csv().fromStream(file);
    console.log('Successfully read CSV file from S3');
  
    console.log('Converting CSV data to DynamoDB JSON format...');
    const pointsData = csvData.map((csvEntry) => {
      const latitude = parseFloat(csvEntry.latitude);
      const longitude = parseFloat(csvEntry.longitude);
      const geohash = generateGeoHash({ latitude, longitude });
      const hashKey = generateHashKey(geohash, HASH_KEY_LENGTH);
      return {
        Item: {
          ...marshall({
            ...csvEntry,
            latitude,
            longitude
          }),
          hashKey: { N: hashKey.toString(10) },
          rangeKey: { S: uuid.v4() },
          geohash: { N: geohash.toString(10) },
          geoJson: {
            S: JSON.stringify({
              type: 'Point',
              coordinates: [csvEntry.longitude, csvEntry.latitude]
            })
          }
        }
      }
    });
    // Convert data to DynamoDB JSON format
    const dynamoDBPointsData = ''.concat(...pointsData.map((item) => JSON.stringify(item) + '\n'));
    console.log('Successfully converted CSV data to DynamoDB JSON format');

    if (CREATE_LOCAL_FILE) {
      console.log(`Writing data to ${LOCAL_FILE_PATH}...`)
      fs.writeFile(LOCAL_FILE_PATH, dynamoDBPointsData, (err) => {
        if (err) {
          console.log(err);
        } else {
          console.log(`Successfully written to ${LOCAL_FILE_PATH}`);
        }
      });
    }
  
    console.log('Uploading data to S3...');
    s3.putObject({
      Bucket: DESTINATION_BUCKET,
      Key: DESTINATION_KEY,
      Body: dynamoDBPointsData,
    }, (err, data) => {
      if (err) {
        console.log(err);
      } else {
        console.log('Successfully uploaded data to S3');
      }
    });
  } catch (err) {
    console.log('Error', err);
  }
};

main();

/*
  You will need to manually import the DynamoDB JSON file in S3 to DynamoDB using the AWS Console.
  To import the data, follow these steps:
  1. Go to the DynamoDB console
  2. Click on "Imports from S3" > "Import from S3"
  3. Select the bucket and file you uploaded to S3
  4. Select "DynamoDB JSON" as the Import file format
  5. Click on "Next"
  6. Enter a name for your new table
  7. Partitions key: "hashKey" (Number), Sort key: "rangeKey" (String)
  8. Under "Table settings", select "Customize settings"
  9. Under "Secondary indexes", click on "Create global index"
  10. Enter "geohash-index" as the index name
  11. Partitions key: "geohash" (Number), Sort key: "geohash" (Number)
  12. Click on "Create index" > "Next" > "Import"
*/

