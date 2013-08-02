var extend = require('node.extend');

function Job(type, data, queue) {
  if(!type) throw new Error('Supply type');
  if(!data) throw new Error('Supply data');
  if(!queue) throw new Error('Supply instance of SuperQueue');
  this._type = type;
  this._data = data;
  this._queue = queue;
  this._failed = false;
  this._removed = false;
  this._completed = false;
}

Job.prototype._transition = function() {
  if(this._queue._processing > 1) {
    // don't shift queue until all concurrent jobs are processed
    --this._queue._processing;
    return;
  }
  --this._queue._processing;
  this._queue._transitionState(this._type);
};

Job.prototype.retry = function() {
  // don't retry completed jobs
  if(this._completed) return;

  var self = this;
  var type = this._type;
  // retry logic for failed jobs
  if(this._failed) {
    // move from failed to inactive
    this._queue.job_client.lrem(this._queue.namespace + ':' + type + ':failed', 1, this._data, function() {
      self._failed = false;
      self._removed = false;

      self._data = JSON.parse(self._data);
      delete self._data._sq_err;
      self._queue.create(type, self._data, self._data._sq_r, self._data._sq_rd);
      self._transition();
    });
  } else if(!this._removed) {
    // should never hit this!
    // for some reason the job got stuck in active state
    // move from active to inactive
    this._queue.job_client.lrem(this._queue.namespace + ':' + type + ':active', 1, this._data, function() {
      self.create(type, self._data);
      self._transition();
    });
  }
};

Job.prototype.complete = function() {
  if(this._failed || this._removed) return;
  var self = this;
  var type = this._type;
  this._queue.job_client.lrem(this._queue.namespace + ':' + type + ':active', 1, this._data, function() {
    self._data = JSON.parse(self._data);
    if(self._data.hasOwnProperty('_sq_err')) {
      delete self._data._sq_err;
    }
    if(self._data.hasOwnProperty('_sq_r')) {
      delete self._data._sq_r;
    }
    if(self._data.hasOwnProperty('_sq_rd')) {
      delete self._data._sq_rd;
    }
    self._data = JSON.stringify(self._data);
    self._queue.job_client.lpush(self._queue.namespace + ':' + type + ':complete', self._data, function() {
      self._completed = true;

      if(self._queue.options.auto_remove_completed_jobs) {
        self.remove();
      } else {
        self._queue.emit('complete', self);
      }

      self._transition();
    });
  });
};

Job.prototype.remove = function() {
  if(this._removed) return;
  // remove this job from inactive, active and complete list
  this._queue.job_client.lrem(this._queue.namespace + ':' + this._type + ':inactive', 1, this._data);
  this._queue.job_client.lrem(this._queue.namespace + ':' + this._type + ':active', 1, this._data);
  this._queue.job_client.lrem(this._queue.namespace + ':' + this._type + ':complete', 1, this._data);
  this._removed = true;
};

Job.prototype.failed = function(options) {
  if(this._failed) return;
  var self = this;
  var type = this._type;

  options = extend({
    err: '',
    data: this._data
  }, options);
  this._failed = true;
  this.remove();

  if(typeof options.data === 'object') {
    this._data = JSON.stringify(options.data);
  } else {
    this._data = options.data;
  }

  var err = options.err;

  if(err) {
    if(err instanceof Error) {
      err = err.stack;
    }
    this._data = JSON.parse(this._data);
    this._data._sq_err = err;
  }

  if(typeof this._data !== 'object') {
    this._data = JSON.parse(this._data);
  }
  if(this._data.hasOwnProperty('_sq_r')) {
    if(this._data._sq_r > 1) {
      --this._data._sq_r;
      var timeout = this._data._sq_rd || 0;
      this._data = JSON.stringify(this._data);
      // try again
      this._queue.job_client.lpush(self._queue.namespace + ':' + type + ':failed', this._data);
      setTimeout(function() {
        self.retry();
      }, timeout);
    } else {
      // failed permanently
      delete this._data._sq_r;
      delete this._data._sq_rd;
      this._data = JSON.stringify(this._data);

      this._queue.job_client.lpush(self._queue.namespace + ':' + type + ':failed', this._data);
      self._queue.emit('failed', self);
      this._transition();
    }
  } else {
    this._data = JSON.stringify(this._data);
    this._queue.job_client.lpush(self._queue.namespace + ':' + type + ':failed', this._data);
    // can only retry a failed job; disabled complete and remove.
    self._queue.emit('failed', self);
    this._transition();
  }
};

module.exports = Job;
