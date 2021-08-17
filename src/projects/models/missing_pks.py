missing_pks = {
    "dataGap":['17071207510493982017-09-01',
               '17071912364675632017-09-01',
               '17081815533768722017-09-01',
               '15082015083892752016-09-01',
               'NML00000_2019_Sandy_5012019-09-01'],

    "dataHeight":['17071912364675632017-09-01',
                  '11062914591449342013-09-01',
                  '11051112543982892013-09-01'],
    "dataLPI":['15082015083892752016-09-01','NML00000_2019_Sandy_5012019-09-01'],
    "dataSoilStability":['15061107204117352015-09-10',
                '15072117415644452015-09-10',
                '1606231547451562016-09-01',
                '15070115065570822015-09-10',
                '15070817194671232015-09-10',
                '15071516212644512015-09-10',
                '15061109502352762015-09-10',
                '15061109503858772015-09-10',
                '15061012474022422015-09-10',
                '15070116214517692015-09-10',
                '1507201842135732015-09-10',
                '15072118230793402015-09-10',
                '15072307072476792015-09-10',
                '15072308002739522015-09-10',
                '15061109503025512015-09-10',
                '15061109502685032015-09-10',
                '15061109501930562015-09-10',
                '15061806553313572015-09-10',
                '15062508012828012015-09-10',
                '15062509535650522015-09-10',
                '15062510492972452015-09-10',
                '15063017120819272015-09-10',
                '15063018160041252015-09-10',
                '17060911393164762017-09-01',
                '17060612044890902017-09-01',
                '17042608593769502017-09-01',
                '17060115430011702017-09-01',
                '18090209051646112018-09-01',
                '18091708400288932018-09-01',
                '18091710495682832018-09-01',
                '18091713033286092018-09-01',
                '18091714593735982018-09-01',
                '17060506454387822018-09-01',
                '17060606191768862018-09-01',
                '17060616532875532018-09-01',
                '17060809112813472018-09-01',
                '17061209222182522018-09-01',
                '17061308574183542018-09-01',
                '17061416352297852018-09-01',
                '17061910100826462018-09-01',
                '17062609240652612018-09-01',
                '17061508304887032018-09-01',
                '17062209010859702018-09-01',
                '17062614033671812018-09-01',
                '17062109501325502018-09-01',
                '17062709145060962018-09-01',
                '17062112502073572018-09-01',
                '17062712435967572018-09-01',
                '17061410543228912018-09-01',
                '17062010165485062018-09-01',
                '17062908371387452018-09-01',
                '1706201449386592018-09-01',
                '17070509424791132018-09-01',
                '17070715414673252018-09-01',
                '1707110759093502018-09-01',
                '17070608581093812018-09-01',
                '17070613111843332018-09-01',
                '17071916331970032018-09-01',
                '1707111324077122018-09-01',
                '17071214220832932018-09-01',
                '17071311182567242018-09-01',
                '17071714394361392018-09-01',
                '17071710384935892018-09-01',
                '17071811375217352018-09-01',
                '17071911292966862018-09-01',
                '17072010411297852018-09-01',
                '17072412535259822018-09-01',
                '17072410040453202018-09-01',
                '17072510382158332018-09-01',
                '17072512541431622018-09-01',
                '17072710263517652018-09-01',
                '17080312323724372018-09-01',
                '17080111044839242018-09-01',
                '17073110363956752018-09-01',
                '17073113135399092018-09-01',
                '17080714234743502018-09-01',
                '17080709570176482018-09-01',
                '17080811511320052018-09-01',
                '17080911020196772018-09-01',
                '170810101241242018-09-01',
                '17080211315185282018-09-01',
                '17081410200798022018-09-01',
                '17081415205215742018-09-01',
                'NML00000_2019_Sandy_5012019-09-01'],
    "geoSpecies":['NML00000_Calibration_NMCalibration2019-09-01'],
    "geoIndicators":['NML00000_Calibration_NMCalibration2019-09-01',
                     'NML00000_Calibration_NMCalibration2019-09-01']
}
fields_to_drop = {
    "dataheader":["ELEVATION"],
    "datagap":["DataEntry","DataErrorChecking", "Observer", "PLOTKEY", "Recorder", "STATE"],
    "dataheight":["DataEntry","DataErrorChecking", "Observer","Recorder","UOM"],
    "datalpi":["DataEntry","DataErrorChecking","HeightOption","HeightUOM","Observer","PLOTKEY","Recorder","SAGEBRUSH_SPP","STATE"],
    "datasoilstability":["DataEntry","DataErrorChecking","Observer","Recorder"],
    "dataspeciesinventory" : ["created_date","created_user","DataEntry","DataErrorChecking", "GlobalID","last_edited_date","last_edited_user","Observer","PLOTKEY","Recorder","SpeciesCount"],
    "geoindicators" : ["created_date", "created_user", "EcolSiteName","ELEVATION", "GlobalID", "last_edited_date","last_edited_user", "PLOTKEY","RecordCount"],
    "geospecies" : ["created_date","created_user", "GlobalID", "last_edited_date", "last_edited_user"]
}
"""
table dependent functions:
"""

table_dep={
    "datagap":"""
            CREATE OR REPLACE FUNCTION public.gap_json(VARIADIC coords character varying[])
                RETURNS SETOF "dataGap"
                LANGUAGE 'sql'
                VOLATILE
                PARALLEL UNSAFE
                COST 100
            AS $BODY$

            SELECT "dataGap"
                FROM (
                  SELECT * FROM "dataHeader" AS "dataHeader"
                  )
                AS "dataHeader"
                LEFT OUTER JOIN "dataGap" AS "dataGap"
                  ON "dataHeader"."PrimaryKey" = "dataGap"."PrimaryKey"

            WHERE postgis.ST_Intersects(
            	"dataHeader".wkb_geometry,
            	postgis.ST_MakePolygon(
            		postgis.ST_SetSRID(
            			postgis.ST_GeomFromText(
            				format('LINESTRING(%s)', VARIADIC "coords")
            			)::postgis.geometry,
            	4326))) = 't'

            $BODY$;""",
    "geoindicators":"""
                    CREATE OR REPLACE FUNCTION public.geoind_json(VARIADIC coords character varying[])
                        RETURNS SETOF "geoIndicators"
                        LANGUAGE 'sql'
                        VOLATILE
                        PARALLEL UNSAFE
                        COST 100
                    AS $BODY$

                    SELECT "geoIndicators"
                        FROM (
                          SELECT * FROM "dataHeader" AS "dataHeader"
                          )
                        AS "dataHeader"
                        LEFT OUTER JOIN "geoIndicators" AS "geoIndicators"
                          ON "dataHeader"."PrimaryKey" = "geoIndicators"."PrimaryKey"

                    WHERE postgis.ST_Intersects(
                    	"dataHeader".wkb_geometry,
                    	postgis.ST_MakePolygon(
                    		postgis.ST_SetSRID(
                    			postgis.ST_GeomFromText(
                    				format('LINESTRING(%s)', VARIADIC "coords")
                    			)::postgis.geometry,
                    	4326))) = 't'

                    $BODY$;
                    """,
    "geospecies":"""
                CREATE OR REPLACE FUNCTION public.geospe_json(VARIADIC coords character varying[])
                    RETURNS SETOF "geoSpecies"
                    LANGUAGE 'sql'
                    VOLATILE
                    PARALLEL UNSAFE
                    COST 100
                AS $BODY$

                SELECT "geoSpecies"
                    FROM (
                      SELECT * FROM "dataHeader" AS "dataHeader"
                      )
                    AS "dataHeader"
                    LEFT OUTER JOIN "geoSpecies" AS "geoSpecies"
                      ON "dataHeader"."PrimaryKey" = "geoSpecies"."PrimaryKey"

                WHERE postgis.ST_Intersects(
                	"dataHeader".wkb_geometry,
                	postgis.ST_MakePolygon(
                		postgis.ST_SetSRID(
                			postgis.ST_GeomFromText(
                				format('LINESTRING(%s)', VARIADIC "coords")
                			)::postgis.geometry,
                	4326))) = 't'

                $BODY$;
                """,
    "dataheader":"""
                CREATE OR REPLACE FUNCTION public.header_json(VARIADIC coords character varying[])
                    RETURNS SETOF "dataHeader"
                    LANGUAGE 'sql'
                    VOLATILE
                    PARALLEL UNSAFE
                    COST 100
                AS $BODY$

                (SELECT *

                FROM
                	public."dataHeader" as dh

                WHERE postgis.ST_Intersects(
                	dh.wkb_geometry,
                	postgis.ST_MakePolygon(
                		postgis.ST_SetSRID(
                			postgis.ST_GeomFromText(
                				format('LINESTRING(%s)', VARIADIC "coords")
                			)::postgis.geometry,
                	4326))) = 't')

                $BODY$;
                """,
    "dataheight":"""
                CREATE OR REPLACE FUNCTION public.height_json(VARIADIC coords character varying[])
                    RETURNS SETOF "dataHeight"
                    LANGUAGE 'sql'
                    VOLATILE
                    PARALLEL UNSAFE
                    COST 100
                AS $BODY$

                SELECT "dataHeight"
                    FROM (
                      SELECT * FROM "dataHeader" AS "dataHeader"
                      )
                    AS "dataHeader"
                    LEFT OUTER JOIN "dataHeight" AS "dataHeight"
                      ON "dataHeader"."PrimaryKey" = "dataHeight"."PrimaryKey"

                WHERE postgis.ST_Intersects(
                	"dataHeader".wkb_geometry,
                	postgis.ST_MakePolygon(
                		postgis.ST_SetSRID(
                			postgis.ST_GeomFromText(
                				format('LINESTRING(%s)', VARIADIC "coords")
                			)::postgis.geometry,
                	4326))) = 't'

                $BODY$;
                """,
    "datalpi":"""
                CREATE OR REPLACE FUNCTION public.lpi_json(VARIADIC coords character varying[])
                    RETURNS SETOF "dataLPI"
                    LANGUAGE 'sql'
                    VOLATILE
                    PARALLEL UNSAFE
                    COST 100
                AS $BODY$

                SELECT "dataLPI"
                    FROM (
                      SELECT * FROM "dataHeader" AS "dataHeader"
                      )
                    AS "dataHeader"
                    LEFT OUTER JOIN "dataLPI" AS "dataLPI"
                      ON "dataHeader"."PrimaryKey" = "dataLPI"."PrimaryKey"

                WHERE postgis.ST_Intersects(
                	"dataHeader".wkb_geometry,
                	postgis.ST_MakePolygon(
                		postgis.ST_SetSRID(
                			postgis.ST_GeomFromText(
                				format('LINESTRING(%s)', VARIADIC "coords")
                			)::postgis.geometry,
                	4326))) = 't'

                $BODY$;
                """,
    "datasoilstability":"""
                        CREATE OR REPLACE FUNCTION public.soilstab_json(VARIADIC coords character varying[])
                            RETURNS SETOF "dataSoilStability"
                            LANGUAGE 'sql'
                            VOLATILE
                            PARALLEL UNSAFE
                            COST 100
                        AS $BODY$

                        SELECT "dataSoilStability"
                            FROM (
                              SELECT * FROM "dataHeader" AS "dataHeader"
                              )
                            AS "dataHeader"
                            LEFT OUTER JOIN "dataSoilStability" AS "dataSoilStability"
                              ON "dataHeader"."PrimaryKey" = "dataSoilStability"."PrimaryKey"

                        WHERE postgis.ST_Intersects(
                        	"dataHeader".wkb_geometry,
                        	postgis.ST_MakePolygon(
                        		postgis.ST_SetSRID(
                        			postgis.ST_GeomFromText(
                        				format('LINESTRING(%s)', VARIADIC "coords")
                        			)::postgis.geometry,
                        	4326))) = 't'

                        $BODY$;
                        """,
    "dataspeciesinventory":"""
                            CREATE OR REPLACE FUNCTION public.specinv_json(VARIADIC coords character varying[])
                                RETURNS SETOF "dataSpeciesInventory"
                                LANGUAGE 'sql'
                                VOLATILE
                                PARALLEL UNSAFE
                                COST 100
                            AS $BODY$

                            SELECT "dataSpeciesInventory"
                                FROM (
                                  SELECT * FROM "dataHeader" AS "dataHeader"
                                  )
                                AS "dataHeader"
                                LEFT OUTER JOIN "dataSpeciesInventory" AS "dataSpeciesInventory"
                                  ON "dataHeader"."PrimaryKey" = "dataSpeciesInventory"."PrimaryKey"

                            WHERE postgis.ST_Intersects(
                            	"dataHeader".wkb_geometry,
                            	postgis.ST_MakePolygon(
                            		postgis.ST_SetSRID(
                            			postgis.ST_GeomFromText(
                            				format('LINESTRING(%s)', VARIADIC "coords")
                            			)::postgis.geometry,
                            	4326))) = 't'

                            $BODY$;
                            """
}
