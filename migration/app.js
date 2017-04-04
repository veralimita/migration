const Menu = require('terminal-menu');
const path = require('path');
const moduler = require('./moduler.json');
const EventEmitter = require('events');

const scripts = [
  { name: 'CREATE LICENSES' },    // 0
  { name: 'CREATE DEVICES' },     // 1
  { name: 'CREATE CONTAINERS' },  // 2
  { separator: true },
  { name: 'EXIT' },
];

class Loader extends EventEmitter {
  constructor() {
    super();

    this.config = require('./config.json');

    this.menu = Menu({ width: 38, x: 4, y: 2 });
    this.menu.reset();
    this.menu.write(' ------------------------------------ \n');
    this.menu.write('| MIGRATION MAPIT SCRIPTS (V1 -> V2) |\n');
    this.menu.write(' ------------------------------------ \n');
    this.menu.write('\n');

    scripts.forEach((s) => {
      s.separator && this.menu.write('----------------------------------\n');
      s.name && this.menu.add(s.name);
    });

    this.menu.on('select', (label, index) => {
      this.menu.close();
      switch (index) {
        case 0 :
        case 1 :
        case 2 :
          let d = require('domain').create();
          d.on('error', (er) => {
            console.error('error, but oh well', er.message);
          });
          d.run(() => {
            const Module = require(path.join(__dirname, moduler[index]));
            new Module(this);
          });
          break;
        default :
          console.log(`${index} doesn´t defined`);
      }
    });

    process.stdin.pipe(this.menu.createStream()).pipe(process.stdout);
    process.stdin.setRawMode(true);

    this.menu.on('close', function () {
      process.stdin.setRawMode(false);
      process.stdin.end();
    });

    this.on('loaded', (message) => {
      console.log(`${message}`);
      this.emit('ready');
    });
  }
}

const loader = new Loader();
