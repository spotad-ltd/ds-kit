var fs = require('fs');
var gplay = require('google-play-scraper');
//var pgpool = require("./pgPool");

/*
var cb_test = function (result) {
        console.log(result)
}

var queryString = fs.readFileSync('./tests/query.sql').toString();
var queryString = "select * from dwh_prd.ssd_20170924 where m_inventory = 'APP' and mobile_os = 'Android' limit 10"; 
pgpool.runPostgresQueryString(queryString, cb_test);
*/
var androidAppsHistory = fs.readFileSync('bundles').toString('utf-8').split("\n");
//var androidAppsHistory = androidAppsHistory.slice(0, 30000)

console.log(androidAppsHistory.length)

//var pgResults = [{shlomi: 'com.jiubang.go.music'}, {shlomi: 'com.nintendo.zara'}];
var outfile = fs.createWriteStream('res_bundles');
var errorsfile = fs.createWriteStream('androidAppsErrors.txt');

let itemsProcessed = 0;
var cb = function (androidApps) {
	//let itemsProcessed = 0;
	console.log(itemsProcessed, i)
    //var androidAppsInfo = [];
    //var errorCatcher = [];
    androidApps.forEach(function(app_, idx){

        gplay.app({appId: app_}).then(function (result, error) {
        itemsProcessed += 1;
            if (!error) {
                //androidAppsInfo.push([result.appId, result.genre, result.price]);
                //console.log('androidAppsInfo', androidAppsInfo.length)
                //console.log('itemsProcessed', itemsProcessed)

                outfile.write([result.appId, result.genre, result.genreId, result.price].join(', ') + '\n')
                //console.log(itemsProcessed)
                //fs.appendFileSync('out1', [result.appId, result.genre, result.price].join(', ') + '\n')
                //console.log("success")
            }
            /*if (itemsProcessed % 100 == 0) {
                    console.log(itemsProcessed.toString() + ' items processed')
            }*/
            if(itemsProcessed == androidAppsHistory.length-1) {
              console.log("Done")
              process.exit(0)
            }
            }).catch(function (e) {
	            //errorCatcher.push(idx, e);
	            errorsfile.write([idx, app_, e.toString()].join(', ') + '\n')
	            itemsProcessed += 1;
            });
    });
    set(i += 120)
};

var i = 0;
function set(i){
	if (i < androidAppsHistory.length) {
		setTimeout(cb, 5000, androidAppsHistory.slice(i, i+120))
	}
}
set(i);
