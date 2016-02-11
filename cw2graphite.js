var config = require('./lib/readConfig.js').readCmdOptions();

// Now using the official Amazon Web Services SDK for Javascript
var AWS = require("aws-sdk");

// We'll use the Cloudwatch API
var cloudwatch;
if (config.awsCredentials != undefined) {
  cloudwatch = new AWS.CloudWatch(config.awsCredentials);
} else {
  cloudwatch = new AWS.CloudWatch();
}

// get the graphite prefix from the metrics config or use cloudwatch as default
var graphitePrefix = config.metricsConfig.carbonNameSpacePrefix || 'cloudwatch';

// use legacy format, defaulting to false
var useLegacyFormat = config.metricsConfig.legacyFormat;

var elasticCacheMetrics = config.elasticCacheMetrics
var RDSMetrics = config.RDSMetrics
var ELBMetrics = config.ELBMetrics
var ECSClusterMetrics = config.ECSClusterMetrics
var ECSServiceMetrics = config.ECSServiceMetrics
var LambdaMetrics = config.LambdaMetrics
// pulling all of lodash for _.sortBy(), does it matter? Do we even need to sort?
var _ = require('lodash');

// TODO: do we need both those libraries, do we need any?
var dateFormat = require('dateformat');
require('./lib/date');

// number of minutes of recent data to query
var interval = 3;

// Between now and 11 minutes ago
var now = new Date();
var then = (interval).minutes().ago();
var end_time = dateFormat(now, "isoUtcDateTime");
var start_time = dateFormat(then, "isoUtcDateTime");

// We used to use this when looking at Billing metrics
// if ( metric.Namespace.match(/Billing/) ) {
//     then.setHours(then.getHours() - 30)
// }
// if ( metric.Namespace.match(/Billing/) ) {
//     options["Period"] = '28800'
// }

var metrics = config.metricsConfig.metrics;

// ELB metrics
getAllELBNames(function(elbs) {
  getELBMetrics(elbs, ELBMetrics);
});

// Elasticache metrics
getAllElasticCacheNames(function(nodes) {
  getElasticCacheMetrics(nodes, elasticCacheMetrics);
});

// RDS metrics
getAllRDSInstanceNames(function(instances) {
  getRDSMetrics(instances, RDSMetrics);
});

// ECS cluster and service metrics - looks nasty
getAllECSClusterARNs(function (clusterarns) {
  clusterarns.forEach(function (clusterarn) {
    getAllECSServiceARNs(clusterarn,function (servicearns) {
      getAllECSClusterNames(clusterarn,function(clustername) {
        getAllECSServiceNames(servicearns,clustername,function (services) {
          getECSClusterMetrics(clustername, ECSClusterMetrics);
          getECSServiceMetrics(services,clustername,ECSServiceMetrics);
        })
      })
    })
  })
})

// Lambda metrics
getAllLambdaFuncNames(function(functions) {
  getLambdaMetrics(functions, LambdaMetrics);
});

for (var index in metrics) {
  printMetric(metrics[index], start_time, end_time);
}

function printMetric(metric, get_start_time, get_end_time) {

  var getMetricStatistics_param = metric;

  getMetricStatistics_param.StartTime = get_start_time;
  getMetricStatistics_param.EndTime = get_end_time;

  cloudwatch.getMetricStatistics(getMetricStatistics_param, function (err, data) {
    if (err) {
      console.error(err, err.stack); // an error occurred
      console.error("on:\n" + JSON.stringify(getMetricStatistics_param, null, 2));
    }
    else {
      formatter = useLegacyFormat ? legacyFormat : newFormat;
      console.log( formatter(metric, data).join("\n"));
    }
  });
}

// Takes the orig query and the response and formats the response as an array of strings
function newFormat(query, data) {
  var dimension_prefix = _.map(query.Dimensions, function(dim) {
    return dim.Name + '_' + dim.Value;
  }).join('.');

  return _.map(data.Datapoints, function(point) {
    var name = query.Namespace.replace("/", ".");
    name += '.' + dimension_prefix;
    name += '.' + query.MetricName;
    var value = point[query['Statistics']];
    var time = parseInt(new Date(point.Timestamp).getTime() / 1000.0);
    return name + ' ' + value + ' ' + time;
  });
}

// Takes the orig query and the response and formats the response as an array of strings
// according to old style of cloudwatch2graphite.
function legacyFormat(query, data) {

  // the legacy format is to only use the dimension Values in the prefix
  var dimension_prefix = _.map(query.Dimensions, function(dim) {
    return dim.Value;
  }).join('.');

  return _.map(data.Datapoints, function(point) {
    var name = query.Namespace.replace("/", ".");
    name += '.' + dimension_prefix;
    name += '.' + query.MetricName;
    name += '.' + query['Statistics'];
    name += '.' + query['Unit'];
    var value = point[query['Statistics']];
    var time = parseInt(new Date(point.Timestamp).getTime() / 1000.0);
    return graphitePrefix + '.' + name.toLowerCase() + ' ' + value + ' ' + time;
  });
}

// returns a hash with all details needed for an cloudwatch metrics query
function buildMetricQuery(namespace, name, unit, statistics, dimensions, period) {
  return {
    'Namespace': namespace,
    'MetricName': name,
    'Unit' : unit,
    'Statistics': [statistics],
    'Dimensions' : dimensions,
    'Period' : period || 60,
  }
}

// executes callback with array of names of all ELBs
function getAllELBNames(callback) {
  var elb = new AWS.ELB(config.awsCredentials);
  elb.describeLoadBalancers({}, function(err, data) {
    if (err) {
      console.log(err);
      callback([]);
    }
    var elbs = _.pluck(data.LoadBalancerDescriptions, 'LoadBalancerName');
    callback(elbs);
  });
}

// takes array of ELB names and gets a variety metrics
function getELBMetrics(elbs) {
  for (index in elbs) {
    var elb = elbs[index];
    var dimensions = [ { "Name" : 'LoadBalancerName', "Value" : elb} ];
    Object.keys(ELBMetrics).forEach(function(unit) {
      ELBMetrics[unit].forEach(function(metric) {
        printMetric(buildMetricQuery('AWS/ELB', metric[0], unit, metric[1], dimensions), start_time, end_time);
      });
    })
  }
}

// executes callback with array of names of all RDS db instances
function getAllRDSInstanceNames(callback) {
  var rds = new AWS.RDS(config.awsCredentials);
  rds.describeDBInstances({}, function(err, data) {
    if (err) {
      console.log(err);
      callback([]);
    }
    var instances = _.pluck(data.DBInstances, 'DBInstanceIdentifier');
    callback(instances);
  });
}

// takes array of RDS db instance names and gets a variety metrics
function getRDSMetrics(instances, RDSMetrics) {
  for (index in instances) {
    var instance = instances[index];
    var dimensions = [ { "Name" : 'DBInstanceIdentifier', "Value" : instance} ];
    Object.keys(RDSMetrics).forEach(function(unit) {
      RDSMetrics[unit].forEach(function(metric) {
        printMetric(buildMetricQuery('AWS/RDS', metric[0], unit, metric[1], dimensions), start_time, end_time);
      });
    })
  }
}

// executes callback with array of hashes of that include ElastiCache CacheClusterId and CacheNodeId
function getAllElasticCacheNames(callback) {
  var ec = new AWS.ElastiCache(config.awsCredentials);
  ec.describeCacheClusters({ ShowCacheNodeInfo: true}, function(err, data) {
    if (err) {
      console.log(err);
      callback([]);
    }
    var nodes = _.map(data.CacheClusters, function(value, key) {
    return [{'Name':'CacheClusterId', 'Value':value.CacheClusterId},
      {'Name':'CacheNodeId', 'Value':value.CacheNodes[0].CacheNodeId}];
    });
    callback(nodes);
  });
}

// takes array of hashes of ElastiCache CacheClusterId and CacheNodeId and gets a variety metrics
function getElasticCacheMetrics(nodes, elasticCacheMetrics) {
  for (index in nodes) {
    var node = nodes[index];
    Object.keys(elasticCacheMetrics).forEach(function(unit) {
      elasticCacheMetrics[unit].forEach(function(metric) {
        printMetric(buildMetricQuery('AWS/ElastiCache', metric[0], unit, metric[1], node), start_time, end_time);
      });
    })
  }
}

// executes a callback with array of ARNs of all ECS clusters
function getAllECSClusterARNs(callback) {
  var ecs = new AWS.ECS(config.awsCredentials);
  var ecsarn = ecs.listClusters({}, function(err, list) {
    if (err) {console.log(err, err.stack);callback([]);}
    var arns = list.clusterArns
    callback(arns);
  });
}


// executes callback with array of names of all ECS clusters
function getAllECSClusterNames(ECSARNs,callback) {
  var ecs = new AWS.ECS(config.awsCredentials);
  if (ECSARNs.constructor === Array) {
    var params = {
      clusters: ECSARNs
    };
  } else {
    var params = {
      clusters: [ECSARNs]
    };
  }
  ecs.describeClusters(params, function(err, data) {
    if (err) {
      console.log(err);
      callback([]);
    }
    var clusters = _.pluck(data.clusters, 'clusterName');
    callback(clusters);
  });
}


// takes array of ECS cluster names and gets a variety of metrics
function getECSClusterMetrics(clusters, ECSClusterMetrics) {
  for (index in clusters) {
    var cluster = clusters[index];
    Object.keys(ECSClusterMetrics).forEach(function(unit) {
      ECSClusterMetrics[unit].forEach(function(metric) {
        var dimension = {
          Name: "ClusterName",Value: cluster
        }
        printMetric(buildMetricQuery('AWS/ECS', metric[0], unit, metric[1], [dimension]), start_time, end_time);
      });
    })
  }
}

function getAllECSServiceARNs(ECSARNs,callback) {
  var ecs = new AWS.ECS(config.awsCredentials);
  if (ECSARNs.constructor === Array) {
    var params = {
      cluster: ECSARNs.toString()
    };
  } else {
    var params = {
      cluster: ECSARNs
    };
  }
  var ecsarn = ecs.listServices(params, function(err, list) {
    if (err) {console.log(err, err.stack);callback([]);}
    var arns = list.serviceArns
    callback(arns);
  });
}

// executes callback with array of names of all ECS services
function getAllECSServiceNames(ECSServicesARNs,ECSClusterName,callback) {
  var ecs = new AWS.ECS(config.awsCredentials);
  if (ECSServicesARNs.length > 0) {
    var params = {
      services : ECSServicesARNs,
      cluster: ECSClusterName.toString()
    };
    ecs.describeServices(params, function(err, data) {
      if (err) {
        console.log(err);
        callback([]);
      }
      var services = _.pluck(data.services, 'serviceName');
      callback(services);
    });
  }
}

// takes array of ECS service names and gets a varity of metrics
function getECSServiceMetrics(services,clustername,ECSServiceMetrics) {
  if (services.length > 0) {
    for (index in services) {
      var service = services[index];
      Object.keys(ECSServiceMetrics).forEach(function(unit) {
        ECSServiceMetrics[unit].forEach(function(metric) {
          var dimension = [
            {Name: "ClusterName",Value: clustername.toString()},
            {Name: "ServiceName",Value: service.toString()}
          ]
          printMetric(buildMetricQuery('AWS/ECS', metric[0], unit, metric[1], dimension), start_time, end_time);
        });
      })
    }
  }
}

// executes callback with array with all Lambda function names
function getAllLambdaFuncNames(callback) {
  var lambda = new AWS.Lambda(config.awsCredentials);
  var lambdafn = lambda.listFunctions({}, function(err, data) {
    if (err) {console.log(err, err.stack);callback([]);}
    var funcnames = _.pluck(data.Functions, 'FunctionName')
    callback(funcnames);
  });
}

// takes array of Lambda function names and gets a variety of metrics
function getLambdaMetrics(functions, LambdaMetrics) {
  for (index in functions) {
    var func = functions[index];
    Object.keys(LambdaMetrics).forEach(function(unit) {
      LambdaMetrics[unit].forEach(function(metric) {
        var dimension = [
          {Name: "FunctionName",Value: func.toString()}
        ]
        printMetric(buildMetricQuery('AWS/Lambda', metric[0], unit, metric[1], dimension), start_time, end_time);
      });
    })
  }
}

