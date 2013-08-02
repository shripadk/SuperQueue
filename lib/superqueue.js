var util = require('util');
var redis = require('redis');
var events = require('events');
var extend = require('node.extend');
var base64id = require('base64id');

var Job = require('./job');
var Scheduler = require('./scheduler');

/**
 * @param {string} namespace
 * @param {Object} options
 * @constructor
 */
function SuperQueue(namespace, options) {
  this.namespace = namespace;
  options = options || {};

  this.options = extend({
    db: 2,
    concurrency: 1,
    scheduler: false,
    catch_errors: false,
    auto_remove_completed_jobs: false
  }, options);

  this.job_client = redis.createClient();
  this.job_client.select(this.options.db);

  // counter to track jobs
  this._processing = 0;
  this._startedProcessing = false;

  this.blocking_clients = {};

  if(this.options.scheduler) {
    this.scheduler = new Scheduler(this, this.namespace, this.options);
  }

  events.EventEmitter.call(this);
}

util.inherits(SuperQueue, events.EventEmitter);

/**
 * Transition job state from inactive to active.
 * Also process jobs that are already in active state.
 *
 * @param {string} type
 * @private
 */
SuperQueue.prototype._transitionState = function(type) {
  var self = this;
  var client = this.blocking_clients[type];
  var inactive_list = this.namespace + ':' + type + ':inactive';
  var active_list = this.namespace + ':' + type + ':active';
  var failed_list = this.namespace + ':' + type + ':failed';

  var processJobs = function() {
    self._startedProcessing = true;
    self.job_client.llen(inactive_list, function(err, len) {
      if(err || !len) {
        // we set to len to 1
        // so that brpoplpush is triggered
        // and is kept blocking on an empty list
        len = 1;
      }

      var concurrency = self.options.concurrency;
      if(self.options.concurrency > len) {
        concurrency = len;
      }

      for(var i=0; i<concurrency; i++) {
        client.brpoplpush(inactive_list, active_list, 0, function(err, data) {
          self.emit(self.namespace+':'+type, data);
        });
      }
      self._processing = concurrency;
    });
  };

  // enter this if block only at process start
  if(!this._startedProcessing) {
    var checkActiveJobs = function() {
      // check if there are any active jobs
      self.job_client.llen(active_list, function(err, len) {
        if(err || !len) {
          // no active jobs so just go ahead with processing!
          processJobs();
          return;
        }

        // found some jobs that are still pending in active state! fetch them!
        self.job_client.lrange(active_list, 0, -1, function(err, jobs) {
          if(err || !jobs) {
            // should never hit this!
            console.error('whoops! something went wrong!');
            return;
          }

          // remove these jobs from active queue
          self.job_client.del(active_list, function() {
            // now move all these jobs to inactive queue again
            jobs.unshift(inactive_list);
            jobs.push(function(err, ok) {
              // no more active jobs pending
              // start processing
              processJobs();
            });
            self.job_client.rpush.apply(self.job_client, jobs);
          });
        });
      });
    };

    // check if there are any failed jobs
    this.job_client.llen(failed_list, function(err, len) {
      if(err || !len) {
        // no failed jobs! check active jobs now.
        checkActiveJobs();
        return;
      }

      // found some jobs that are still pending in failed state! fetch them!
      self.job_client.lrange(failed_list, 0, -1, function(err, jobs) {
        if(err || !jobs) {
          // should never hit this!
          console.error('whoops! something went wrong!');
          return;
        }

        // remove these jobs from failed queue
        self.job_client.del(failed_list, function() {
          // now move all these jobs to inactive queue again
          jobs.unshift(inactive_list);
          jobs.push(function(err, ok) {
            // no more failed jobs pending
            // check active jobs now
            checkActiveJobs();
          });
          self.job_client.rpush.apply(self.job_client, jobs);
        });
      });
    });
  } else {
    processJobs();
  }
};

/**
 * Create a new client for the particular type.
 * This client uses BRPOPLPUSH for switching job states
 * and processing the active job queue.
 *
 * @param {string} type
 * @private
 */
SuperQueue.prototype._createBlockingClient = function(type) {
  // should never hit this
  if(!type) {
    throw new Error('blockingClient: Provide a type');
    return;
  }

  // don't create more than one connection for a type
  if(this.blocking_clients[type]) return;

  var self = this;
  var client = redis.createClient();
  client.select(this.options.db, function() {
    self.blocking_clients[type] = client;
    self._transitionState(type);
  });
};

/**
 * Create a new job and add it to the inactive list
 *
 * @param {string} type
 * @param {Object} data
 * @param {Function} opt_callback
 * @param {number} opt_retries
 * @param {number} opt_retry_delay
 */
SuperQueue.prototype.create = function(type, data, opt_callback, opt_retries, opt_retry_delay) {
  var self = this;
  // unique superqueue id to differentiate job
  if(!data.hasOwnProperty('_sq_id')) {
    data._sq_id = base64id.generateId();
  }

  if(typeof opt_callback === 'number') {
    opt_retry_delay = opt_retries;
    opt_retries = opt_callback;
    opt_callback = function() {};
  }

  if(opt_retries) {
    data._sq_r = opt_retries;
  }

  if(opt_retry_delay) {
    data._sq_rd = opt_retry_delay;
  }

  this.job_client.sadd(self.namespace, type);
  // add job to inactive list
  this.job_client.lpush(self.namespace + ':' + type + ':inactive', JSON.stringify(data), function() {
    if(opt_callback) opt_callback(data._sq_id);
  });
};

/**
 * Delayed Job
 * @param {string} type
 * @param {Object} job
 * @param {number} delay in milliseconds
 * @param {number} opt_retries
 * @param {number} opt_retry_delay
 */
SuperQueue.prototype.delayed_job = function(type, job, delay, opt_retries, opt_retry_delay) {
  if(this.scheduler) {
    this.scheduler.delayed_job.apply(this.scheduler, arguments);
  }
};

/**
 * Process job queue
 *
 * @param {string} type
 * @param {Function} callback
 */
SuperQueue.prototype.process = function(type, callback) {
  var self = this;
  this.on(this.namespace+':'+type, function(data) {
    var job = new Job(type, data, self);

    // catches error if not caught inside the callback
    // this prevents the process from crashing
    if(self.catch_errors) {
      try {
        callback(JSON.parse(data), job);
      } catch(e) {
        job.failed({
          err: e
        });
      }
    } else {
      callback(JSON.parse(data), job);      
    }
  });

  this._createBlockingClient(type);
};

module.exports = SuperQueue;
