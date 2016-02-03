"use strict";

var u = require("underscore");
var riemann = require("riemann");

/*
 * Config object should contain:
 * host, port
 * Optional:
 * transport, flush, packet
 */


function Riemann(startupTime, config, emitter) {
  this.config = config.riemann;
  this.emitter = emitter;

  if (this.config.transport && this.config.transport === "tcp") {
    setInterval(this.reconnectIfNeeded.bind(this), this.config.reconnectInterval);
  } else {
    this.setupRiemannClient();
  }
}

Riemann.prototype.reconnectIfNeeded = function() {
    if (!this.client) {
        if (this.config.debug) {
            console.log('Connecting to Reimann');
        }

        this.setupRiemannClient();
    } else if (this.client.tcp.socket.readyState == 'closed') {
        if (this.config.debug) {
            console.log('Reimann socket seems to be closed, reconnecting');
        }

        this.setupRiemannClient();
    }
}

Riemann.prototype.setupRiemannClient = function() {
  this.client = riemann.createClient({
    host: this.config.host,
    port: this.config.port
  });

  // Bind some Riemann handlers
  this.client.once("connect", u.bind(this.onConnect, this));
  this.client.on("error", u.bind(this.onError, this));
  this.client.on("data", u.bind(this.onAck, this));

  this.emitter.on("packet", u.bind(this.onPacket, this));
}

Riemann.prototype.onConnect = function () {
  if (this.config.debug) {
    console.log("Connected to Riemann client!");
  }
};

Riemann.prototype.onError = function (err) {
    console.error("Socket error:", err);
};

Riemann.prototype.onAck = function (ack) {
  if (this.config.debug) {
    console.log("Received ACK...");
  }
};

Riemann.prototype.onPacket = function (packet, rinfo) {
  var events = this.parsePacket(packet);
  u.each(events, this.handlePacketEvent, this);
};

Riemann.prototype.parsePacket = function (packet) {
  return packet.toString().trim().split("\n");
};

// If `parseNamespace` is set to `true`, `service` is inferred from
// first level `eventString`. Otherwise the entire `eventString` is used.
Riemann.prototype.getService = function (eventString) {
  if (this.config.parseNamespace) {
    return eventString.split(".")[0];
  }

  return eventString.split(":")[0];
};

// If `parseNamespace` is set to `true`, `description` is inferred from
// second level `eventString`. Otherwise the entire `eventString` is used.
Riemann.prototype.getDescription = function (eventString) {
  if (this.config.parseNamespace) {
    var eventStringParts = eventString.split(".");
    eventStringParts.shift();
    eventString = eventStringParts.join(".");
    return eventString.split(":")[0];
  }

  return eventString.split(":")[0];
};

Riemann.prototype.getMetric = function (eventString) {
  var eventMetric = eventString.split(":")[1];
  return eventMetric.split("|")[0];
};

Riemann.prototype.getTags = function (eventString) {
  var tags = [];

  // Include any config tags
  if (this.config.tags && this.config.tags.length > 0) {
    tags.push(this.config.tags);
  }

  if (this.config.tagWithEventParts) {
    tags.push(eventString.split(":")[0].split("."));
  }

  return u.flatten(tags);
};

Riemann.prototype.handlePacketEvent = function (eventString) {
  // See here for valid event properties:
  // http://aphyr.github.io/riemann/concepts.html
  this.send({
    service: this.getService(eventString),
    state: "ok",
    description: this.getDescription(eventString),
    tags: this.getTags(eventString),
    metric: this.getMetric(eventString),
    ttl: this.config.ttl
  });
};

Riemann.prototype.send = function (eventData) {
  var eventToSend = this.client.Event(eventData);

  try {
      // Send using proper transport
      if (this.config.transport && this.config.transport === "tcp") {
        this.client.send(eventToSend, this.client.tcp);
      } else {
        this.client.send(eventToSend);
      }
  } catch (err) {
    if (this.config.debug) {
        console.error('Error while sending, ignoring error:', err.message);
    }
  }
};

module.exports = Riemann;
