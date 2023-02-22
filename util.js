// Adapted from dynamodb-geo.js
// Source: https://github.com/robhogan/dynamodb-geo.js/blob/master/src/dynamodb/DynamoDBManager.ts#L132
const { S2LatLng, S2Cell } = require('nodes2ts');

const generateGeoHash = (geoPoint) => {
  const latLng = S2LatLng.fromDegrees(geoPoint.latitude, geoPoint.longitude);
  const cell = S2Cell.fromLatLng(latLng);
  const cellId = cell.id;
  return cellId.id;
};

const generateHashKey = (geohash, hashKeyLength) => {
  if (geohash.lessThan(0)) {
    // Counteract "-" at beginning of geohash.
    hashKeyLength++;
  }
  const geohashString = geohash.toString(10);
  const denominator = Math.pow(10, geohashString.length - hashKeyLength);
  return geohash.divide(denominator);
};

module.exports = {
  generateGeoHash,
  generateHashKey
}