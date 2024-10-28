## POST 创建数据源

POST /v1/datos/datasource/create

> Body Parameters

```json
{
  "type": "jdbc_mysql",
  "name": "mysql2",
  "description": "mysql test",
  "config": {
    "url": "jdbc:mysql://192.168.1.6:3306/data-center?characterEncoding=utf-8&useSSL=false",
    "username": "root",
    "password": "123456"
  }
}
```

### Params

|Name|Location|Type|Required|Description|
|---|---|---|---|---|
|body|body|object| no |none|
|» type|body|string| yes |none|
|» name|body|string| yes |none|
|» description|body|string| yes |none|
|» config|body|object| yes |none|
|»» url|body|string| yes |none|
|»» username|body|string| yes |none|
|»» password|body|string| yes |none|

> Response Examples

> 200 Response

```json
{}
```

### Responses

|HTTP Status Code |Meaning|Description|Data schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|成功|Inline|

### Responses Data Schema

## DELETE 删除数据源

DELETE /v1/datos/datasource/1

> Response Examples

> 200 Response

```json
{}
```

### Responses

|HTTP Status Code |Meaning|Description|Data schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|成功|Inline|

### Responses Data Schema

## POST 测试数据源

POST /v1/datos/datasource/check/connect

> Body Parameters

```json
{
  "type": "jdbc_mysql",
  "name": "mysql2",
  "description": "mysql test",
  "config": {
    "url": "jdbc:mysql://192.168.1.6:3306/data-center?characterEncoding=utf-8&useSSL=false",
    "username": "root",
    "password": "123456"
  }
}
```

### Params

|Name|Location|Type|Required|Description|
|---|---|---|---|---|
|body|body|object| no |none|
|» type|body|string| yes |none|
|» name|body|string| yes |none|
|» description|body|string| yes |none|
|» config|body|object| yes |none|
|»» url|body|string| yes |none|
|»» username|body|string| yes |none|
|»» password|body|string| yes |none|

> Response Examples

> 200 Response

```json
{}
```

### Responses

|HTTP Status Code |Meaning|Description|Data schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|成功|Inline|

### Responses Data Schema

## GET 获取数据源详细信息

GET /v1/datos/datasource/2

> Response Examples

> 200 Response

```json
{}
```

### Responses

|HTTP Status Code |Meaning|Description|Data schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|成功|Inline|

### Responses Data Schema

## PUT 编辑数据源

PUT /v1/datos/datasource/2

> Body Parameters

```json
{
  "type": "jdbc_mysql",
  "name": "mysql",
  "description": "mysql test",
  "config": {
    "url": "jdbc:mysql://192.168.1.6:3306/data-center?characterEncoding=utf-8&useSSL=false",
    "username": "root",
    "password": "123456"
  }
}
```

### Params

|Name|Location|Type|Required|Description|
|---|---|---|---|---|
|body|body|object| no |none|
|» type|body|string| yes |none|
|» name|body|string| yes |none|
|» description|body|string| yes |none|
|» config|body|object| yes |none|
|»» url|body|string| yes |none|
|»» username|body|string| yes |none|
|»» password|body|string| yes |none|

> Response Examples

> 200 Response

```json
{}
```

### Responses

|HTTP Status Code |Meaning|Description|Data schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|成功|Inline|

### Responses Data Schema

## GET 获取数据源列表

GET /v1/datos/datasource/selectPage

> Body Parameters

```json
{
  "dsName": null,
  "businessId": null,
  "createBeginTime": null,
  "createEndTime": null
}
```

### Params

|Name|Location|Type|Required|Description|
|---|---|---|---|---|
|pageIndex|query|string| yes |none|
|pageSize|query|string| yes |none|
|body|body|object| no |none|
|» dsName|body|null| yes |none|
|» businessId|body|null| yes |none|
|» createBeginTime|body|null| yes |none|
|» createEndTime|body|null| yes |none|

> Response Examples

> 200 Response

```json
{}
```

### Responses

|HTTP Status Code |Meaning|Description|Data schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|成功|Inline|

### Responses Data Schema

## GET 获取数据源动态表单

GET /v1/datos/datasource/dynamic-form

### Params

|Name|Location|Type|Required|Description|
|---|---|---|---|---|
|type|query|string| yes |none|

> Response Examples

> 200 Response

```json
{}
```

### Responses

|HTTP Status Code |Meaning|Description|Data schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|成功|Inline|



