const fs = require("node:fs");
const path = require("node:path");

const candidates = [
  "journio_node_native.node",
  "journio-node-native.node",
  "index.node",
  `journio_node_native.${process.platform}-${process.arch}.node`,
  `journio-node-native.${process.platform}-${process.arch}.node`
];

for (const candidate of candidates) {
  const fullPath = path.join(__dirname, candidate);
  if (fs.existsSync(fullPath)) {
    module.exports = require(fullPath);
    return;
  }
}

throw new Error(
  "Journio native binding is not built. Run `npm run build:native` in bindings/nodejs."
);
