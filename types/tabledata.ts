// BigQuery API v2 - tabledata.list types
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list

/**
 * Request query parameters for tabledata.list endpoint
 */
type TableDataListQueryParams = {
  /**
   * Maximum number of rows to return per page
   */
  maxResults?: number;

  /**
   * Token for pagination; retrieves next result set
   */
  pageToken?: string;

  /**
   * Zero-based row index to begin results
   */
  startIndex?: string;

  /**
   * Comma-separated list of fields to include
   */
  selectedFields?: string;

  /**
   * Return TIMESTAMP as INT64 milliseconds since epoch
   */
  "formatOptions.useInt64Timestamp"?: boolean;
};

/**
 * Path parameters for tabledata.list endpoint
 */
type TableDataListPathParams = {
  /**
   * GCP project identifier
   */
  projectId: string;

  /**
   * The dataset containing the table
   */
  datasetId: string;

  /**
   * The table from which to retrieve rows
   */
  tableId: string;
};

/**
 * A single field value in a table row
 */
type TableCell = {
  v: unknown;
};

/**
 * A single row in the table
 */
type TableRow = {
  /**
   * Array of field values in column order
   */
  f: TableCell[];
};

/**
 * Response from tabledata.list endpoint
 */
type TableDataList = {
  /**
   * Resource type identifier
   */
  kind: string;

  /**
   * Entity tag for caching
   */
  etag: string;

  /**
   * Collection of table rows
   */
  rows?: TableRow[];

  /**
   * Token for retrieving subsequent pages
   */
  pageToken?: string;

  /**
   * Total row count in table
   */
  totalRows?: string;
};

export type {
  TableDataListQueryParams,
  TableDataListPathParams,
  TableDataList,
  TableRow,
  TableCell,
};
