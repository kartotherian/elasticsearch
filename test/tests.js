'use strict';

const assert = require('assert');
const Promise = require('bluebird');
const elasticsearch = require('elasticsearch');
const lib = require('../ElasticSearch');
const Err = require('@kartotherian/err');

describe('Tests', function() {

  Promise.config({
    warnings: true,
    longStackTraces: true
  });

  const host = 'localhost:9200';
  const log = 'debug';
  const esClient = new elasticsearch.Client({host/*, log*/});

  const tilelive = {
    protocols: {}
  };

  lib.registerProtocols(tilelive);

  function createIndexName() {
    let prefix = 'tiletest';
    let indexCount = 0;

    const tryOne = () => {
      const index = prefix + (indexCount ? `_${indexCount}` : '');
      return esClient.indices.exists({index})
        .then((exists) => {
          if (!exists) return index;
          indexCount++;
          return tryOne();
        });
    };

    return tryOne();
  }

  function cleanup(index) {
    if (index === undefined) {
      return;
    }
    return esClient.indices
      .delete({index})
      .catch(
        (err) => console.error('Cleanup failed', err)
      );
  }

  function runTest(testFunc) {
    let index;

    return createIndexName()
      .then(
        idx => {
          return esClient.indices
            .create({
              index: idx,
              body: {
                mappings: {
                  tile: {
                    properties: {
                      body: {type: 'binary'}
                    }
                  }
                }
              }
            })
            .then(() => {
              index = idx;
              return index;
            });
        })
      .then(() => new lib({query: {log, host, index}}).init())
      .then(testFunc)
      .then(
        () => cleanup(index),
        (err) => {
          return cleanup(index).then(() => {
            throw err;
          })
        });
  }

  it('info', () => runTest(
    inst => {
      assert.deepStrictEqual(tilelive, {
        protocols: {
          'elasticsearch:': tilelive.protocols['elasticsearch:']
        }
      });

      return Promise.resolve()
        .then(() => inst.getAsync({type: 'info'}))
        .then((inf) => {
          assert.strictEqual(typeof inf, 'object');
          assert.strictEqual(typeof inf.data, 'object');
          assert.strictEqual(inf.data.bounds, '-180,-85.0511,180,85.0511');
          assert.strictEqual(inf.data.minzoom, 0);
          assert.strictEqual(inf.data.maxzoom, 22);
        })
        .then(() => inst.putInfoAsync({test: 123}))
        .then(() => inst.getAsync({type: 'info'}))
        .then((inf) => assert.deepStrictEqual(inf, {data: {test: 123}}));
    })
  );

  it('tile', () => runTest(
    inst => Promise.resolve()
      .then(() => inst.getAsync({z: 0, x: 0, y: 0}))
      .then(
        () => assert.fail('should not exist'),
        (err) => assert(Err.isNoTileError(err), 'must be isNoTileError'))
      .then(() => inst.putTileAsync(0, 0, 0, new Buffer('abc')))
      .then(() => inst.getAsync({z: 0, x: 0, y: 0}))
      .then((res) => {
        assert.deepStrictEqual(res,
          {
            data: new Buffer('abc'),
            headers: {
              'Content-Type': 'application/x-protobuf',
              'Content-Encoding': 'gzip'
            }
          });
      })));
});
