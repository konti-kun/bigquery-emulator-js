import type { TableFieldSchema } from "./query";

type TableReference = {
  projectId: string;
  datasetId: string;
  tableId: string;
};

type Clustering = {
  fields: string[];
};

type TimePartitioning = {
  type: "HOUR" | "DAY" | "MONTH" | "YEAR";
  expirationMs?: string;
  field?: string;
  requirePartitionFilter?: boolean;
};

type RangePartitioning = {
  field: string;
  range: {
    start: string;
    end: string;
    interval: string;
  };
};

type ViewDefinition = {
  query: string;
  userDefinedFunctionResources?: Array<{
    resourceUri: string;
    inlineCode: string;
  }>;
  useLegacySql?: boolean;
  useExplicitColumnNames?: boolean;
};

type MaterializedViewDefinition = {
  query: string;
  lastRefreshTime?: string;
  enableRefresh?: boolean;
  refreshIntervalMs?: string;
  allowNonIncrementalDefinition?: boolean;
};

type TableSchema = {
  fields: TableFieldSchema[];
};

type ExternalDataConfiguration = {
  sourceUris: string[];
  schema?: TableSchema;
  sourceFormat: string;
  maxBadRecords?: number;
  autodetect?: boolean;
  ignoreUnknownValues?: boolean;
  compression?: string;
  csvOptions?: {
    fieldDelimiter?: string;
    skipLeadingRows?: string;
    quote?: string;
    allowQuotedNewlines?: boolean;
    allowJaggedRows?: boolean;
    encoding?: string;
  };
  googleSheetsOptions?: {
    skipLeadingRows?: string;
    range?: string;
  };
  bigtableOptions?: {
    ignoreUnspecifiedColumnFamilies?: boolean;
    readRowkeyAsString?: boolean;
    columnFamilies?: Array<{
      familyId?: string;
      type?: string;
      encoding?: string;
      columns?: Array<{
        qualifierEncoded?: string;
        qualifierString?: string;
        fieldName?: string;
        type?: string;
        encoding?: string;
        onlyReadLatest?: boolean;
      }>;
      onlyReadLatest?: boolean;
    }>;
  };
  hivePartitioningOptions?: {
    mode?: string;
    sourceUriPrefix?: string;
    requirePartitionFilter?: boolean;
    fields?: string[];
  };
  connectionId?: string;
  decimalTargetTypes?: string[];
  avroOptions?: {
    useAvroLogicalTypes?: boolean;
  };
  jsonOptions?: {
    encoding?: string;
  };
  parquetOptions?: {
    enumAsString?: boolean;
    enableListInference?: boolean;
  };
  fileSetSpecType?: string;
  referenceFileSchemaUri?: string;
  metadataCacheMode?: string;
};

type StreamingBuffer = {
  estimatedBytes?: string;
  estimatedRows?: string;
  oldestEntryTime?: string;
};

type EncryptionConfiguration = {
  kmsKeyName: string;
};

type SnapshotDefinition = {
  baseTableReference: TableReference;
  snapshotTime: string;
};

type CloneDefinition = {
  baseTableReference: TableReference;
  cloneTime: string;
};

type Table = {
  kind: string;
  etag: string;
  id: string;
  selfLink: string;
  tableReference: TableReference;
  friendlyName?: string;
  description?: string;
  labels?: Record<string, string>;
  schema?: TableSchema;
  timePartitioning?: TimePartitioning;
  rangePartitioning?: RangePartitioning;
  clustering?: Clustering;
  requirePartitionFilter?: boolean;
  numBytes?: string;
  numPhysicalBytes?: string;
  numLongTermBytes?: string;
  numLongTermPhysicalBytes?: string;
  numRows?: string;
  creationTime: string;
  expirationTime?: string;
  lastModifiedTime: string;
  type: "TABLE" | "VIEW" | "EXTERNAL" | "MATERIALIZED_VIEW" | "SNAPSHOT" | "CLONE";
  view?: ViewDefinition;
  materializedView?: MaterializedViewDefinition;
  materializedViewStatus?: {
    lastRefreshStatus?: {
      reason?: string;
      location?: string;
      debugInfo?: string;
      message?: string;
    };
    refreshWatermark?: string;
  };
  externalDataConfiguration?: ExternalDataConfiguration;
  location: string;
  streamingBuffer?: StreamingBuffer;
  encryptionConfiguration?: EncryptionConfiguration;
  snapshotDefinition?: SnapshotDefinition;
  cloneDefinition?: CloneDefinition;
  defaultCollation?: string;
  defaultRoundingMode?:
    | "ROUNDING_MODE_UNSPECIFIED"
    | "ROUND_HALF_AWAY_FROM_ZERO"
    | "ROUND_HALF_EVEN";
  maxStaleness?: string;
  resourceTags?: Record<string, string>;
  tableConstraints?: {
    primaryKey?: {
      columns: string[];
    };
    foreignKeys?: Array<{
      name?: string;
      referencedTable: TableReference;
      columnReferences: Array<{
        referencingColumn: string;
        referencedColumn: string;
      }>;
    }>;
  };
  biglakeConfiguration?: {
    connectionId: string;
    storageUri: string;
    fileFormat: string;
    tableFormat: string;
  };
  replicas?: Array<{
    datasetId: string;
    projectId: string;
    tableId: string;
    replicaStatus?:
      | "REPLICA_STATUS_UNSPECIFIED"
      | "ACTIVE"
      | "SOURCE_DELETED"
      | "PERMISSION_DENIED"
      | "UNSUPPORTED_CONFIGURATION";
  }>;
};

type PostTableRequest = Omit<
  Table,
  | "kind"
  | "etag"
  | "id"
  | "selfLink"
  | "numBytes"
  | "numPhysicalBytes"
  | "numLongTermBytes"
  | "numLongTermPhysicalBytes"
  | "numRows"
  | "creationTime"
  | "lastModifiedTime"
  | "location"
  | "streamingBuffer"
  | "materializedViewStatus"
>;

export type {
  Table,
  PostTableRequest,
  TableReference,
  TableSchema,
  ViewDefinition,
  MaterializedViewDefinition,
  ExternalDataConfiguration,
  TimePartitioning,
  RangePartitioning,
  Clustering,
  EncryptionConfiguration,
  SnapshotDefinition,
  CloneDefinition,
  StreamingBuffer,
};
