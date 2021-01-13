var http = require('http');
var fs = require('fs');

var fs = require('fs');
var es = require('event-stream');
var _ = require('lodash');

const STREAM_FILE_NAME = 'data.json';
let totalLines = 0;
let blockedLined = [];
let playerLevelMap = new Map();//will tell you guys why I need this.

/*
Please note that this progarm is designed for demo ONLY, will add more abilities for production like:
Unit test, Error handler, Nestjs Queue, Nestjs Logging, Nestjs Processor etc.
*/

function run() {
  readStream(STREAM_FILE_NAME);
}

function readStream(fileName) {
  let stream = fs.createReadStream(fileName);
  stream.setEncoding('UTF-8');

  stream.pipe(es.split())
    .pipe(
      es.mapSync(async function (line) {
        try {
          if (line) {
            let passedText = await processer(line);
            if (passedText) {
              console.log(`passed text: ${passedText}`);
            }
          }
        }
        catch (error) {
          console.error('Json parse error:', error);
        }

      }).on('error', function (err) {
        console.error('Reading file error:', err);
      }).on('end', function () {
        console.log(`${totalLines} lines read.\n`);
        console.log(`${blockedLined.length} players blocked:\n`);
        console.log(playerLevelMap);
      })
    )
}

async function processer(data) {
  if (_.isEmpty(data)) {
    return;
  }

  totalLines++;

  let obj = JSON.parse(data);
  let topics = obj['topics'] || [];
  let averageRelevanceValue = topics.reduce((pre, v) => pre += v['relevance'], 0) / topics.length;
  let player = { blocked: 0, passed: 0 };

  if (playerLevelMap.has(obj['player']))
    player = playerLevelMap.get(obj['player']);

  if (obj['filtered'] === 1) { // blocked line
    let blocked = {};
    blocked['player'] = obj['player'];
    blocked['flag'] = obj['flag'];
    blocked['averageRelevance'] = averageRelevanceValue;
    blockedLined.push(blocked);
    player['blocked']++;
    return;
  }
  //none-blocked player
  player['passed']++;
  playerLevelMap.set(obj['player'], { ...player });
  return obj['text'] || null;
}

run();