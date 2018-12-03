// aws_s3_queue

// documentation via: haraka -c /Users/slobodandjordjevic/projects/haraka -h plugins/aws_s3_queue

// Put your plugin code here
// type: `haraka -h Plugins` for documentation on how to create a plugin

const AWS = require("aws-sdk"),
    zlib = require("zlib"),
    util = require('util'),
    async = require("async"),
    Transform = require('stream').Transform;

  const TransformStream = function() {
      Transform.call(this);
  };
  util.inherits(TransformStream, Transform);
  
  TransformStream.prototype._transform = function(chunk, encoding, callback) {
      this.push(chunk);
      callback();
  };

exports.register = function () {
  this.logdebug("Initializing AWS S3 Queue");

  const config = this.config.get("aws_s3_queue.json");
  this.logdebug("Config loaded : "+util.inspect(config));

  AWS.config.update({
      accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey, region: config.region
  });

  this.s3Bucket = config.s3Bucket;

  this.zipBeforeUpload = config.zipBeforeUpload;
  this.fileExtension = config.fileExtension;
  this.copyAllAddresses = config.copyAllAddresses;
  };

  exports.hook_queue = function (next, connection) {
    const plugin = this;

    const transaction = connection.transaction;
    const emailTo = transaction.rcpt_to;

    const gzip = zlib.createGzip();
    const transformer = plugin.zipBeforeUpload ? gzip : new TransformStream();
    const body = transaction.message_stream.pipe(transformer);

    const s3 = new AWS.S3();

    const addresses = plugin.copyAllAddresses ? transaction.rcpt_to : transaction.rcpt_to[0];

    async.each(addresses, function (address, eachCallback) {
        const key = address.user + "@" + address.host + "/" + transaction.uuid + plugin.fileExtension;

        const params = {
            Bucket: plugin.s3Bucket,
            Key: key,
            Body: body
        };

        s3.upload(params).on('httpUploadProgress', function (evt) {
            plugin.logdebug("Uploading file... Status : " + util.inspect(evt));
        }).send(function (err, data) {
            plugin.logdebug("S3 Send response data : " + util.inspect(data));
            eachCallback(err);
        });
    }, function (err) {
        if (err) {
            plugin.logerror(err);
            next();
        } else {
            next(OK, "Email Accepted.");
        }
    });
};

exports.shutdown = function () {
  this.loginfo("Shutting down queue plugin.");
};