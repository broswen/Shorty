'use strict';

const { DynamoDB, RemoteCredentials } = require('aws-sdk');
const AWS = require('aws-sdk');
const md5 = require('md5');
const DYNAMO = new AWS.DynamoDB.DocumentClient();

module.exports.shorten = async event => {
  let body;
  try {
    body = JSON.parse(event.body);
    if (!('url' in body)) {
      throw new Error('must specify url');
    }
  } catch (error) {
    return {
      statusCode: 400,
      body: JSON.stringify(
        { message: error.message }
      )
    }
  }

  let url = body.url;
  // counter to salt the hash, so identical links don't get the same hash
  let salt = Date.now();

  let hash;
  let unique = false;

  while (!unique) {
    // check for hash collision, linear probing
    // hash url with salt and use first 6 characters
    hash = md5(`${url}${salt}`).substring(0,6);

    const params2  = {
      Key: {
        _link : hash
      },
      TableName: process.env.LINKTABLE
    }

    // check if hash is already in use
    await DYNAMO.get(params2).promise()
      .then(data => {
        if(data.Item == undefined){
          unique = true;
        } else {
          console.warn("hash collision", data)
          if (data.Item._timeout < Date.now()){
            // old link, replace it
            unique = true;
          } else {
            // if something is returned there is a collision
            // increment salt
            salt += 1;
          }
        }
      })
      .catch(err => {
        return {
          statusCode: 500,
          body: JSON.stringify(
            {
              message: error.message,
            }
          ),
        };
      });
  }

  const params = {
    Item: {
      "_link": hash,
      "_url": url,
      "_timeout": Date.now() + (1000 * 60)
    },
    TableName: process.env.LINKTABLE
  }

  try {
    await DYNAMO.put(params).promise();
  } catch (error) { 
    return {
      statusCode: 500,
      body: JSON.stringify(
        {
          message: error.message,
        }
      ),
    };
  }

  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        url: `${event.headers.Host}/l/${hash}`,
      }
    ),
  };
};

module.exports.get = async event => {

  console.log(event);

  const query = event.pathParameters;
  if(!('link' in query)) {
    return {
      statusCode: 400,
      body: JSON.stringify(
        {
          message: 'missing link parameter',
        }
      ),
    };
  }

  const params = {
    Key: {
      "_link": query.link
    },
    TableName: process.env.LINKTABLE
  }

  let data;
  try {
    data = await DYNAMO.get(params).promise();
  } catch (error) {
    return {
      statusCode: 404,
      body: JSON.stringify(
        {
          message: 'link not found',
        }
      ),
    }; 
  }

  if (data.Item == undefined || data.Item._timeout < Date.now()) {
    return {
      statusCode: 404,
      body: JSON.stringify(),
    };
  }

  return {
    statusCode: 301,
    headers: {
      Location: data.Item._url
    },
    body: JSON.stringify(),
  };
};