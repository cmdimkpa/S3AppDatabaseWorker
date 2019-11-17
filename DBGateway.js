var express = require('express');
var fs = require('fs')
var https = require('https')
var bodyParser = require('body-parser');
var path = require('path');
var cors = require('cors')
var request = require('request');
var compression = require('compression');
var app = express();

// CORS
app.options('*', cors())

// port
var port = __DB_GATEWAY_PORT__;

// middleware

	// bodyParser
	app.use(bodyParser.json());
	app.use(bodyParser.urlencoded({extended:false}));

	//CORS
	app.use(function(req, res, next) {
  	res.header("Access-Control-Allow-Origin", "*");
  	res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  	next();
	});

	// compression
	app.use(compression())

	// catch errors
	app.use(function (error, req, res, next) {
  		if (error instanceof SyntaxError) {
  			// Syntax Error
    		res.status(400);
    		res.json({code:400,message:"Malformed request. Check and try again."});
  		} else {
  			// Other error
  			res.status(400);
  			res.json({code:400,message:"There was a problem with your request."});
    	next();
  	}
});

// Mask configuration
function maskURL(){
  return "https://__DB_SERVER_HOST__:__DB_SERVER_PORT__"   // replace with actual mask URL
}

//routes

app.get('*',(req,res)=>{
  var actionUrl = maskURL()+req.url;
  request(
    {url:actionUrl,method:"GET",json:true},
      (error,response,body)=>{
        res.json(body)
        }
      );
  });

  app.post('*',(req,res)=>{
    var actionUrl = maskURL()+req.url;
    request(
      {url:actionUrl,method:"POST",json:true,body:req.body},
        (error,response,body)=>{
          res.json(body)
          }
        );
    });

https.createServer({
  key: fs.readFileSync('__SERVER_KEYFILE__'),
  cert: fs.readFileSync('__SERVER_CERTFILE__')
}, app)
.listen(port,()=>{
  console.log('server running on port '+port);
});
