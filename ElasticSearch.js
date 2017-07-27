'use strict';

/*
 ElasticSearch tile storage source for Kartotherian
 */

const util = require('util');
const Promise = require('bluebird');
const elasticsearch = require('elasticsearch');
const promistreamus = require('promistreamus');
const qidx = require('quadtile-index');
const checkType = require('@kartotherian/input-validator');
const Err = require('@kartotherian/err');
const pckg = require('./package.json');
const uptile = require('tilelive-promise');

let prepared = {prepare: true};

class ElasticSearch {
  constructor(uri) {
    uptile(this);
    this.batchMode = 0;
    this.batch = [];
    this.headers = {
      'Content-Type': 'application/x-protobuf',
      'Content-Encoding': 'gzip'
    };

    const esOptions = {};
    const params = checkType.normalizeUrl(uri).query;

    if (!params.host) {
      throw new Err("Uri must include at least one 'host' connect point query parameter");
    }
    esOptions.host = params.host;

    if (typeof params.log === 'string') {
      esOptions.log = params.log;
    }

    if (typeof params.index !== 'string') {
      throw new Err("Uri must have a valid 'index' query parameter");
    }
    this.index = params.index;

    this.createIfMissing = !!params.createIfMissing;
    this.type = 'tile';

    this.minzoom = typeof params.minzoom === 'undefined' ? 0 : parseInt(params.minzoom);
    this.maxzoom = typeof params.maxzoom === 'undefined' ? 22 : parseInt(params.maxzoom);
    this.maxBatchSize = typeof params.maxBatchSize === 'undefined' ? undefined : parseInt(params.maxBatchSize);

    this.client = new elasticsearch.Client(esOptions);
  }

  init() {
    return this.client.ping().then(() => this);
  }

  getAsync(opts) {

    return Promise.try(() => {
      switch (opts.type) {
        case undefined:
        case 'tile':
          return Promise.try(() => {
            if (opts.z < this.minzoom || opts.z > this.maxzoom) Err.throwNoTile();
            return this._getTileAsync(ElasticSearch._makeId(opts.z, opts.x, opts.y));
          }).then(data => {
            if (!data) Err.throwNoTile();
            return {data, headers: this.headers};
          });
          break;
        case 'info':
          return this
            ._getTileAsync('info')
            .then(data => {
              if (data) {
                return {data: JSON.parse(data.toString())};
              } else {
                return {
                  data: {
                    'tilejson': '2.1.0',
                    'name': 'ElasticSearch ' + pckg.version,
                    'bounds': '-180,-85.0511,180,85.0511',
                    'minzoom': this.minzoom,
                    'maxzoom': this.maxzoom
                  }
                };
              }
            });
        default:
          throw new Err('unknown type ' + opts.type);
          break;
      }
    });
  }

  static _makeId(z, x, y) {
    return `${z}_${x}_${y}`;
  }

  _getTileAsync(id) {
    return Promise
      .try(() => this.client.get({index: this.index, type: this.type, id: id}))
      .then((doc) => {
        if (doc.found) {
          return new Buffer(doc._source.data, 'base64');
        } else {
          return null;
        }
      }, (err) => {
        if (err.status === 404) return null;
        throw err;
      });
  }

  putInfo(data, callback) {
    return this.putTileAsync(data).nodeify(callback);
  }

  putInfoAsync(data) {
    return Promise.try(() => this._putTileAsync('info', new Buffer(JSON.stringify(data))));
  }

  putTile(z, x, y, tile, callback) {
    return this.putTileAsync(z, x, y, tile).nodeify(callback);
  }

  putTileAsync(z, x, y, data) {
    return Promise.try(() => {
      if (z < this.minzoom || z > this.maxzoom) {
        throw new Err('This ElasticSearch source cannot save zoom %d, because its configured for zooms %d..%d',
          z, this.minzoom, this.maxzoom);
      }
      return this._putTileAsync(ElasticSearch._makeId(z, x, y), data);
    });
  }

  _putTileAsync(id, data) {
    return Promise.try(() => {
      const opt = {index: this.index, type: this.type, id: id};
      if (!data || data.length === 0) {
        return this.client.delete(opt);
      }

      opt.body = {data: data.toString('base64')};
      if (!this.batchMode || !this.maxBatchSize) {
        return this.client.create(opt);
      } else {
        throw new Err('bulk not implemented');
        // this.batch.push({query: query, params: params});
        // if (Object.keys(this.batch).length > this.maxBatchSize) {
        //   return this.flushAsync();
        // }
      }
    });
  }

  close(callback) {
    let cl = this.client;
    if (!cl) {
      callback(null);
    } else {
      Promise.try(
        () => (this.batchMode && this.maxBatchSize) ? this.flushAsync() : true
      ).then(() => {
        delete this.client;
        this.batchMode = 0;
        // return cl.shutdownAsync();
      }).nodeify(callback);
    }
  }

  startWriting(callback) {
    this.batchMode++;
    callback(null);
  }

  flush(callback) {
    return callback();

    // let batch = this.batch;
    // if (Object.keys(batch).length > 0) {
    //   this.batch = [];
    //   this.client
    //     .batchAsync(batch, prepared)
    //     .nodeify(callback);
    // } else {
    //   callback();
    // }
  }

  stopWriting(callback) {
    Promise.try(() => {
      if (this.batchMode === 0) {
        throw new Err('stopWriting() called more times than startWriting()')
      }
      this.batchMode--;
      return this.flushAsync();
    }).nodeify(callback);
  }

  static registerProtocols(tilelive) {
    tilelive.protocols['elasticsearch:'] = (uri, callback) => {
      return Promise.try(() => {
        const es = new ElasticSearch(uri);
        return es.init();
      }).nodeify(callback);
    }
  }
}

module.exports = ElasticSearch;
