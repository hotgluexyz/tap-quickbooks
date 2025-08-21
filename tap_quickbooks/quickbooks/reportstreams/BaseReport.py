from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream

import singer

LOGGER = singer.get_logger()

class BaseReportStream(QuickbooksStream):
    
    def __init__(self, qb, start_date, report_periods, state_passed):
        self.qb = qb
        self.start_date = start_date
        self.has_number_of_periods = report_periods is not None
        self.number_of_periods = report_periods or 3
        self.state_passed = state_passed
    
    def concurrent_get(self, report_entity, params):
        log_msg = f"Fetch {report_entity} for period {params['start_date']} to {params['end_date']}"
        LOGGER.info(log_msg)
        response = self._get(report_entity, params)
        LOGGER.info(f"COMPLETE: {log_msg}")

        if "Unable to display more data. Please reduce the date range." in str(
            response
        ):
            return {
                "error": "Too much data for current period",
                "start_date": params["start_date"],
                "end_date": params["end_date"],
            }
        else:
            return response
    
    def _get_column_metadata(self, resp, schema=None):
        columns = []
        for column in resp.get("Columns").get("Column"):
            # To handle multiple languages if schema is passed, always convert Col Titles to english
            if schema is not None:
                col_type = column["MetaData"][0].get("Value") if column.get("MetaData") else None
                if not col_type:
                    LOGGER.info(f"Metadata for col {column.get("ColTitle")} not found, skipping.")
                    continue
                # append col to columns
                columns.append(schema.get(col_type))
            else:
                if column.get("ColTitle") == "Memo/Description":
                    columns.append("Memo")
                else:
                    columns.append(column.get("ColTitle").replace(" ", ""))
        columns.append("Categories")
        return columns
