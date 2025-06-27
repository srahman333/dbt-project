
/* 
Configarations are nothing but , those are the values for  that particular DBT Model.
"materialized":'table', --> We ewant to create this  particular code asa table inside  our snowflake. Thats why we pass materilization inside our table.
"pre_hook": copy_json('COUNTRY_DETAILS_CP'), --> Passing a pre hook statement means any statement you wanna  run before the  below  "with cte statement" , you need to pass it to pre hook. So prehook statement run first and then the selow select statement run.
copy_json('COUNTRY_DETAILS_CP') --> copy_jason is the macro we created in previous "copy into sql file"

"schema": 'RAW'

*/



{{

config

({
"materialized":'table',
"pre_hook": copy_json('COUNTRY_DETAILS_CP'),
"schema": 'RAW'
})
}}

 

WITH country_details_raw AS

(

SELECT X.VALUE AS SOURCE_DATA,

CURRENT_TIMESTAMP(6) AS INSERT_DTS

FROM {{source('country','COUNTRY_DETAILS_CP')}} A,

LATERAL FLATTEN (A.DATA) X

)

 

SELECT

CAST(SOURCE_DATA AS VARIANT) AS SOURCE_DATA,

CAST(INSERT_DTS AS TIMESTAMP(6)) AS INSERT_DTS

FROM country_details_raw

