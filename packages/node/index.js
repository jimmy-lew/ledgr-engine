const { Engine } = require('./ledgr-engine.node');

function createEngine(path) {
  return new Engine(path);
}

module.exports = {
  Engine,
  createEngine,
};
