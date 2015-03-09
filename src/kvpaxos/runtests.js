var spawn = require('child_process').spawn;
var exec = require('child_process').exec;
var execSync = require('child_process').execSync;
var fs = require('fs');
var numTest = parseInt(process.argv[2]);
var removeTemps = process.argv[2];
execSync('rm *.txt');

fs.writeFileSync('master.txt', '');
var files = [];
var closed = 0;
for(var i=0;i<numTest;i++){
  var fileName = 'TEST'+ i.toString() + Math.random().toString(26)  + '.txt';
  files.push(fileName);
  (function(fileName){
    var test = spawn('go', ['test']);
    test.stdout.on('data', function(data){
      fs.appendFile(fileName, data);
    })
    test.stderr.on('data', function(data){
      var stringed = String(data);
      if(stringed.indexOf('method') < 0 && stringed.indexOf('die') < 0){
        fs.appendFile(fileName, data);
      }
    })
    test.on('close', function(){
      closed ++;
      if(closed === numTest){
        combineFiles(files);
      }
    })
  })(fileName);
};

function combineFiles(files){
  files.forEach(function(el){
    fs.readFile(el, function(err, data){
      if(!err){
        // Append to master logs.
        fs.appendFile('master.txt', data);
      }
      // Remove temp file after done
      if(removeTemps){
        exec('rm ' + el);
      }
    })
  })
}
