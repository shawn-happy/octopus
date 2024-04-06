# 创建Job
* url: http://<ip>:<port>/data-workflow/api/v1/job-definition
* Request Method: `POST`
* Request Body:
```json
{
    "jobName": "first-job",
    "description": "作业描述",
    "source": {
        "name": "first-source",
        "options": {
            "fullType": "source_pulsar",
            "pluginType": "SOURCE",
            "identify": "pulsar",
            "clientUrl": "pulsar://192.168.54.206:6650,192.168.54.207:6650,192.168.54.208:6650",
            "adminUrl": "http://192.168.54.206:8080,192.168.54.207:8080,192.168.54.208:8080",
            "token": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.yY2MSTBSEk3WkL3yEfSUdyq-EbeO1Lyg5EG1KiuGO4M",
            "topic": "319wxg_10w_5",
            "subscription": "wxg-test-5",
            "subscriptionType": "Earliest"
        },
        "output": "pulsar-output"
    },
    "transforms": [
        {
            "name": "generated-field",
            "options": {
                "fullType": "transform_generatedField",
                "pluginType": "TRANSFORM",
                "identify": "generatedField",
                "fields": [
                    {
                        "algo": "CURRENT_TIME",
                        "destName": "now",
                        "type": "STRING"
                    },
                    {
                        "algo": "UUID",
                        "destName": "uuid",
                        "type": "STRING"
                    }
                ]
            },
            "input": [
                "pulsar-output"
            ],
            "output": "generate-output"
        }
    ],
    "sink": {
        "name": "first-sink",
        "options": {
            "fullType": "sink_jdbc",
            "pluginType": "SINK",
            "identify": "jdbc",
            "url": "jdbc:mysql://192.168.54.206:3306/test?characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true",
            "username": "root",
            "password": "bigdata321",
            "driver": "com.mysql.cj.jdbc.Driver",
            "database": "test",
            "table": "test_wxg_10w_319_5",
            "batchSize": 1000,
            "mode": "INSERT",
            "fields": [
                "sequenceId",
                "uuid",
                "now"
            ]
        },
        "input": [
            "generate-output"
        ]
    }
}
```
* Response Body:
```json
{
    "code": 0,
    "msg": "6b5001384d684a298587f90cadfa4f82", # 作业定义ID
    "data": null
}
```

# 根据ID查看作业定义
* url: http://localhost:8080/data-workflow/api/v1/job-definition/<jobDefId>
* Request Method: `GET`
* Request Params: 
  * jobDefId: 作业定义ID
* Response Body:
```json
{
  "code": 0,
  "msg": null,
  "data": {
    "jobId": "6b5001384d684a298587f90cadfa4f82",
    "name": "first-job",
    "description": null,
    "source": {
      "stepId": "703f28e927c3464da97a6f8dcacde45e",
      "stepName": "first-source",
      "type": "source",
      "identifier": "pulsar",
      "description": null,
      "input": null,
      "output": "pulsar-output",
      "attributes": [
        {
          "attributeId": "363e58dc798c4cb4a6d582894d98ad4e",
          "code": "clientUrl",
          "value": "pulsar://192.168.54.206:6650,192.168.54.207:6650,192.168.54.208:6650",
          "version": "V1"
        },
        {
          "attributeId": "4fb96a943fca4a578b26d12b77fafcf1",
          "code": "subscriptionType",
          "value": "Earliest",
          "version": "V1"
        },
        {
          "attributeId": "5ba1187fe30f422a9541389485295dce",
          "code": "adminUrl",
          "value": "http://192.168.54.206:8080,192.168.54.207:8080,192.168.54.208:8080",
          "version": "V1"
        },
        {
          "attributeId": "727d7aa1fe2f4b8bb235fbd2dfc3084b",
          "code": "topic",
          "value": "319wxg_10w_5",
          "version": "V1"
        },
        {
          "attributeId": "784b04f889054bcaa07eeed26330ae73",
          "code": "token",
          "value": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.yY2MSTBSEk3WkL3yEfSUdyq-EbeO1Lyg5EG1KiuGO4M",
          "version": "V1"
        },
        {
          "attributeId": "cb370aab1dc844e0b012d3dd3a3cb695",
          "code": "subscription",
          "value": "wxg-test-5",
          "version": "V1"
        }
      ],
      "version": "V1"
    },
    "transforms": [
      {
        "stepId": "868360e4c3d04bb0b18870d6f96ceb25",
        "stepName": "generated-field",
        "type": "transform",
        "identifier": "generatedField",
        "description": null,
        "input": [
          "pulsar-output"
        ],
        "output": "generate-output",
        "attributes": [
          {
            "attributeId": "e130260cdfea43e09b27e98d0272844f",
            "code": "fields",
            "value": "[{\"algo\":\"CURRENT_TIME\",\"field\":null,\"type\":\"STRING\"},{\"algo\":\"UUID\",\"field\":null,\"type\":\"STRING\"}]",
            "version": "V1"
          }
        ],
        "version": "V1"
      }
    ],
    "sink": {
      "stepId": "a61662be4b08472591fa02e8b70c28a5",
      "stepName": "first-sink",
      "type": "sink",
      "identifier": "jdbc",
      "description": null,
      "input": [
        "generate-output"
      ],
      "output": null,
      "attributes": [
        {
          "attributeId": "01ba65cf71804f51850efe973b874830",
          "code": "password",
          "value": "bigdata321",
          "version": "V1"
        },
        {
          "attributeId": "048f2c76f0c14283950e0aa5ca9ef4a2",
          "code": "primaryKeys",
          "value": "null",
          "version": "V1"
        },
        {
          "attributeId": "0ffda245e2ad442890b48e249a0f2cfa",
          "code": "conditionFields",
          "value": "null",
          "version": "V1"
        },
        {
          "attributeId": "49d92ae7bf2c4ce589f04d7d3247dd07",
          "code": "username",
          "value": "root",
          "version": "V1"
        },
        {
          "attributeId": "73eb1208dc2f4e6eb36826e6e064104b",
          "code": "fields",
          "value": "sequenceId,uuid,now",
          "version": "V1"
        },
        {
          "attributeId": "7e63e781203c46f8bc6fd43208bdebfa",
          "code": "url",
          "value": "jdbc:mysql://192.168.54.206:3306/test?characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true",
          "version": "V1"
        },
        {
          "attributeId": "8706dbc3fb454fe9a86fda7d7530bae1",
          "code": "mode",
          "value": "INSERT",
          "version": "V1"
        },
        {
          "attributeId": "b4ee99f429144e478decbaea27ae4c60",
          "code": "table",
          "value": "test_wxg_10w_319_5",
          "version": "V1"
        },
        {
          "attributeId": "c79b76bebb4e4fc0b6abb67b694b4fc0",
          "code": "database",
          "value": "test",
          "version": "V1"
        },
        {
          "attributeId": "d481bbdf63ae49448f6d2db29228f45a",
          "code": "driver",
          "value": "com.mysql.cj.jdbc.Driver",
          "version": "V1"
        }
      ],
      "version": "V1"
    },
    "version": "V1"
  }
}
```

# 根据ID删除作业
* url: http://localhost:8080/data-workflow/api/v1/job-definition/<jobDefId>
* Request Method: `DELETE`
* Request Params:
  * jobDefId: 作业定义ID
* Response Body:
```json
{
    "code": 0,
    "msg": "6b5001384d684a298587f90cadfa4f82",
    "data": null
}
```