var spawn = require('child_process').spawn;
var fs = require('fs');
var numTest = parseInt(process.argv[2]);

fs.writeFileSync('master.txt', '');
var files = [];
var closed = 0;
for(var i=0;i<numTest;i++){
  var fileName = 'testfiles' + Math.random().toString(26) + i.toString() + '.txt';
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
    fs.appendFile('master.txt', fs.readFileSync(el))
  })
}
