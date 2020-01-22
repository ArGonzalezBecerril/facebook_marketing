
--Author:      Arturo Gonzalez Becerril
--Descripcion: Crea las tablas a los cuales se ingestara la informacion.

CREATE TABLE FB_DETALLE_CAMPANIA(
ID                  number(30),
CAN_USE_SPEND_CAP   varchar2(15),
CONFIGURED_STATUS   varchar2(15),
CREATED_TIME        varchar2(30),
EFFECTIVE_STATUS    varchar2(15),
LIFETIME_BUDGET     number(15)
NAME                varchar2(400),
OBJETIVE            varchar2(30),
PACING_TYPE         varchar2(20),
SOURCE_CAMPAIGN_ID  number(40),
SPECIAL_AD_CATEGORY varchar2(15),
START_TIME          varchar2(30),
STATUS              varchar2(15),
TOPLINE_ID          varchar2(10),
UPDATED_TIME        varchar2(35),
DATA_DATE_PART      varchar2(30)
);


CREATE TABLE FB_ESTADISTICA_DE_CAMPANIA(
ACCOUNT_ID              number(25),
ACCOUNT_NAME            varchar2(20),
CAMPAIGN_ID             number(25),
CAMPAIGN_NAME           varchar2(400),
CLICKS                  number(15),
CONVERSION_RATE_RANKING varchar2(15),
DATE_START              varchar2(20),
DATE_STOP               varchar2(20),
FREQUENCY               number(20,5),
IMPRESSIONS             number(20),
INLINE_LINK_CLICKS      number(10),
INLINE_POST_ENGAGEMENT  number(10),
OBJECTIVE               varchar2(15),
QUALITY_RANKING         varchar2(15),
REACH                   number(15),
SOCIAL_SPEND            number(15,5),
SPEND                   number(20,5),
UNIQUE_CLICKS           number(15),
DATA_DATE_PART          varchar2(30)
);



CREATE TABLE FB_DETALLE_ANUNCIOS(
CAMPAIGN_ID      number(25), 
CREATED_TIME     varchar2(35),
EFFECTIVE_STATUS varchar2(30),
ID               number(30),
NAME             varchar2(200),
UPDATED_TIME     varchar2(40),
DATA_DATE_PART   varchar2(40)
);


CREATE TABLE FB_ESTADISTICA_ANUNCIOS(
AD_ID                             number(40),
AD_NAME                           varchar2(300),
ADSET_ID                          number(40),
ADSET_NAME                        varchar2(300),
CAMPAIGN_ID                       number(40),
CLICKS                            number(20),
COST_PER_INLINE_LINK_CLICK        number(30,5),
COST_PER_INLINE_POST_ENGAGEMENT   number(40,5),
COST_PER_UNIQUE_CLICK             number(40,5),
COST_PER_UNIQUE_INLINE_LINK_CLICK number(40,5),
CPC                               number(40,5),
CPM                               number(40,5),
CPP                               number(40,5),
CTR                               number(40,5),
DATE_START                        varchar2(20),
DATE_STOP                         varchar2(20),
FREQUENCY                         number(20,5),
IMPRESSIONS                       number(15),
INLINE_LINK_CLICK_CTR             number(20,5),
INLINE_LINK_CLICKS                number(15),
INLINE_POST_ENGAGEMENT            number(15),
OBJECTIVE                         varchar2(15),
REACH                             number(15),
SOCIAL_SPEND                      number(15,5),
SPEND                             number(20,5),
UNIQUE_CLICKS                     number(15),
UNIQUE_CTR                        number(20,5),
UNIQUE_INLINE_LINK_CLICK_CTR      number(20,5),
UNIQUE_INLINE_LINK_CLICKS         number(10),
UNIQUE_LINK_CLICKS_CTR            number(20,5),
DATA_DATE_PART                    varchar2(20)
);

