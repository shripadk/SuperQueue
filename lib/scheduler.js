var util = require('util');
var redis = require('redis');
var events = require('events');
var extend = require('node.extend');
var base64id = require('base64id');

/**
 * @param {SuperQueue} superqueue
 * @param {string} namespace
 * @param {Object} options
 * @constructor
 */
function Scheduler(superqueue, namespace, options) {
  this.namespace = namespace;
  this.superqueue = superqueue;
  this.sched_namespace = '__scheduler__'+namespace+':';
  this.sched_namespace_hash = '__scheduler__'+namespace+'__hash';

  options = options || {};

  this.options = extend({
    db: 2
  }, options);

  this.client = redis.createClient();
  this.client.select(this.options.db);

  this.subscriber_client = redis.createClient();
  this.subscriber_client.select(this.options.db);

  this._setup();

  events.EventEmitter.call(this);
}

util.inherits(Scheduler, events.EventEmitter);

/**
 * @private
 */
Scheduler.prototype._setup = function() {
  var db = this.options.db;
  var client = this.client;
  var queue = this.superqueue;
  var namespace = this.namespace;
  var sched_namespace = this.sched_namespace;
  var subscriber_client = this.subscriber_client;
  var sched_namespace_hash = this.sched_namespace_hash;
  var schannel = '__keyspace@'+db+'__:'+sched_namespace;

  client.config('get', 'notify-keyspace-events', function(e, r) {
    if(e || !r.length) {
      // K = keyspace events, x = Expired events
      client.config('set', 'notify-keyspace-events', 'Kx');
    }

    var setup_subscriber = function() {
      var script = "\
      local g = redis.pcall('hkeys', KEYS[1]) \
      local h = {} \
      for i, key in ipairs(g) do \
        local evalue = redis.pcall('get', key) \
        if not evalue then \
          local value = redis.pcall('hget', KEYS[1], key) \
          local parsed = cjson.decode(value) \
          local type = parsed.__type \
          redis.pcall('hdel', KEYS[1], key) \
          redis.pcall('lpush', KEYS[2] .. ':' .. type .. ':inactive', value) \
        end \
      end \
      return h \
      ";

      client.eval(script, 2, sched_namespace_hash, namespace, function() {
        subscriber_client.on('pmessage', function(pattern, channel, key) {
          // key expired! fetch job from stored hash
          channel = channel.replace(schannel, '');

          client.hget(sched_namespace_hash, channel, function(err, job) {
            if(err || !job) {
              // This job must have already been moved by another scheduler
              return;
            }

            // delete this field from the hash store
            client.hdel(sched_namespace_hash, channel, function(e, r) {
              if(e || !r) return;
              // continue only when job is removed!
              job = JSON.parse(job);

              var type = job.__type;
              var retries = job.__rt;
              var retry_delay = job.__rtd;

              delete job.__type;
              delete job.__rt;
              delete job.__rtd;

              queue.create(type, job, retries, retry_delay);
            });
          });
        });

        // subscribe for keyspace events (only for keys that have expired)
        subscriber_client.psubscribe(schannel+'*');
      });
    };

    if(!r[1].length) {
      client.config('set', 'notify-keyspace-events', 'Kx', setup_subscriber);
      return;
    }

    setup_subscriber();
  });
};

/**
 * @param {string} type
 * @param {Object} job
 * @param {number} delay in milliseconds
 * @param {number} opt_retries
 * @param {number} opt_retry_delay
 */
Scheduler.prototype.delayed_job = function(type, job, delay, opt_retries, opt_retry_delay) {
  // unique id for this job
  var id = base64id.generateId();

  job.__type = type;
  job.__rt = opt_retries;
  job.__rtd = opt_retry_delay;

  var client = this.client;
  var sched_namespace = this.sched_namespace;
  var sched_namespace_hash = this.sched_namespace_hash;
  // we don't set a value as the expired key is not going to be readable anyways
  client.psetex(sched_namespace+id, delay, 1, function() {
    // store the job in a hash
    client.hset(sched_namespace_hash, id, JSON.stringify(job));
  });
};

module.exports = Scheduler;
