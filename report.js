const _ = require('lodash')
const {BigQuery} = require('@google-cloud/bigquery')
const {Storage} = require('@google-cloud/storage')
//const { reject } = require('lodash')
const storage = new Storage()
const bucket = storage.bucket('reporting-stroeer-labs')
var date

exports.handler = async (object, db) => {

  const file = object.name.toLowerCase()
  if (file.match(/\.[0-9a-z]+$/i)[0] === '.json')
  {
    try{
      const jsonContent = JSON.parse(await readJson(bucket.file(object.name))) 
      date = getDate(object.name)
    
      let categoriesCount = jsonContent.data_source_hadoop.categories.categories_count
      categoriesCount = categoriesCount.map((x)=> {return fixCategoriesCount(x)})
    
      let categoriesUrls = jsonContent.data_source_hadoop.categories.url_analysis
      categoriesUrls = categoriesUrls.map((x)=> {return fixCategoriesUrls(x)})
    
      let categoriesMetrics = jsonContent.data_source_hadoop.categories
      categoriesMetrics = fixCategoriesMetrics(categoriesMetrics,jsonContent)
  
      let segmentCounts = jsonContent.data_source_hadoop.segments.segments_count
      segmentCounts = segmentCounts.map((x)=> {return fixSegmentCounts(x)})
  
      let segmentMetrics = jsonContent.data_source_hadoop.segments
      segmentMetrics = fixSegmentMetrics(segmentMetrics)
   

      console.log("Uploading")

      loadToBQ(categoriesCount,"stroeer_labs_contextual_categories_counts")
      loadToBQ(categoriesUrls,"stroeer_labs_contextual_categories_urls")
      loadToBQ(categoriesMetrics,"stroeer_labs_contextual_categories_metrics")
      loadToBQ(segmentCounts,"stroeer_labs_segments_counts")
      loadToBQ(segmentMetrics,"stroeer_labs_segments_metrics")

    } catch (e){
      console.error(e)
    }
  }
  
}

async function readJson(file)
{
  return new Promise((resolve,reject) => {
    let buffer = ''
    file.createReadStream()
      .on('data',(d)=> {buffer += d})
      .on('end', () =>{ resolve(buffer)})
      .on('error', (e)=> {reject(e)})
  })
}


async function loadToBQ(rows,tableName)
{
  try{
    let bigquery = new BigQuery()
    let res
    res = await bigquery
    .dataset("reporting")
    .table(tableName)
    .insert(rows)
    
    console.log(`-------------Uploaded to ${tableName}`)
  } catch(error){
      console.log(JSON.stringify(error))
  }  
}

function getDate(fileName)
{
  let date  = fileName.split("_")
  date = date[2]
  date = date.substring(0,date.length - 5)
  return date
}

function addDate(obj)
{
  obj["date"] = date
  return obj
}

function fixNumbers(obj)
{
    for(attribute in obj)
        if(!isNaN(obj[attribute]) && !Number.isInteger(obj[attribute]))
            obj[attribute]= Math.round(obj[attribute] * 1000000000)/1000000000
        
    return obj
}

function fixCategoriesCount(obj)
{
    let tempObj = {}
    tempObj.category = obj[0]
    tempObj.count = obj[1]
    tempObj = addDate(tempObj)
    return tempObj
}

function fixCategoriesUrls(obj)
{
    let tempObj = {}
    tempObj.url = obj[0]
    tempObj.count = obj[2]
    tempObj.share = obj[3]
    tempObj = fixNumbers(tempObj)
    tempObj = addDate(tempObj)
    if (obj[1] !== "")
    {
      if(obj[1].includes(","))
        tempObj.categories = obj[1].split(",")
      else
      {
        tempObj.categories = []
        tempObj.categories.push(obj[1])
      }  
    }
    return tempObj
}

function fixCategoriesMetrics(obj,json)
{
    delete obj["url_analysis"]
    delete obj["categories_count"]
    obj["total_predictions"] = json.data_source_hadoop.total_predictions
    obj = fixNumbers(obj)
    obj = addDate(obj)
    return obj
}

function fixSegmentCounts(obj)
{
    let tempObj = {}
    tempObj.segment = obj[0]
    tempObj.count = obj[1]
    tempObj = addDate(tempObj)
    return tempObj
}

function fixSegmentMetrics(obj)
{
    delete obj["segments_count"]
    obj = fixNumbers(obj)
    obj = addDate(obj)
    return obj
}