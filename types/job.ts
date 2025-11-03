import type { TableReference, TableSchema } from "./table";

type JobReference = {
  projectId: string;
  jobId: string;
  location?: string;
};

type ErrorProto = {
  reason: string;
  location?: string;
  debugInfo?: string;
  message: string;
};

type JobStatistics = {
  creationTime: string;
  startTime?: string;
  endTime?: string;
  totalSlotMs?: string;
  finalExecutionDurationMs?: string;
  totalBytesProcessed?: string;
  numChildJobs?: string;
  rowLevelSecurityStatistics?: {
    rowLevelSecurityApplied: boolean;
  };
  dataMaskingStatistics?: {
    dataMaskingApplied: boolean;
  };
  completionRatio?: number;
  quotaDeferments?: string[];
  parentJobId?: string;
  sessionInfo?: {
    sessionId: string;
  };
  transactionInfo?: {
    transactionId: string;
  };
  reservationUsage?: Array<{
    name: string;
    slotMs: string;
  }>;
  scriptStatistics?: {
    stackFrames?: Array<{
      startLine?: number;
      startColumn?: number;
      endLine?: number;
      endColumn?: number;
      procedureId?: string;
      text?: string;
    }>;
    evaluationKind?: "EVALUATION_KIND_UNSPECIFIED" | "STATEMENT" | "EXPRESSION";
  };
  jobQueryStats?: {
    estimatedBytesProcessed?: string;
    totalPartitionsProcessed?: string;
    totalBytesProcessed?: string;
    totalBytesBilled?: string;
    billingTier?: number;
    numDmlAffectedRows?: string;
    ddlOperationPerformed?: string;
    ddlTargetTable?: TableReference;
    ddlTargetRowAccessPolicy?: {
      projectId: string;
      datasetId: string;
      tableId: string;
      policyId: string;
    };
    ddlTargetRoutine?: {
      projectId: string;
      datasetId: string;
      routineId: string;
    };
    ddlTargetDataset?: {
      projectId: string;
      datasetId: string;
    };
    totalSlotMs?: string;
    cacheHit?: boolean;
    queryPlan?: Array<{
      name?: string;
      id?: string;
      status?: string;
      shuffleOutputBytes?: string;
      shuffleOutputBytesSpilled?: string;
      recordsRead?: string;
      recordsWritten?: string;
      parallelInputs?: string;
      completedParallelInputs?: string;
      startMs?: string;
      endMs?: string;
      slotMs?: string;
      waitMsAvg?: string;
      waitMsMax?: string;
      readMsAvg?: string;
      readMsMax?: string;
      writeMsAvg?: string;
      writeMsMax?: string;
      computeMsAvg?: string;
      computeMsMax?: string;
      waitRatioAvg?: number;
      waitRatioMax?: number;
      readRatioAvg?: number;
      readRatioMax?: number;
      writeRatioAvg?: number;
      writeRatioMax?: number;
      computeRatioAvg?: number;
      computeRatioMax?: number;
      steps?: Array<{
        kind?: string;
        substeps?: string[];
      }>;
    }>;
    timeline?: Array<{
      elapsedMs?: string;
      totalSlotMs?: string;
      pendingUnits?: string;
      completedUnits?: string;
      activeUnits?: string;
      estimatedRunnableUnits?: string;
    }>;
    referencedTables?: TableReference[];
    referencedRoutines?: Array<{
      projectId: string;
      datasetId: string;
      routineId: string;
    }>;
    schema?: TableSchema;
    dmlStats?: {
      insertedRowCount?: string;
      deletedRowCount?: string;
      updatedRowCount?: string;
    };
    undeclaredQueryParameters?: Array<{
      name?: string;
      parameterType?: {
        type: string;
        arrayType?: any;
        structTypes?: Array<{
          name?: string;
          type?: any;
          description?: string;
        }>;
      };
      parameterValue?: {
        value?: string;
        arrayValues?: any[];
        structValues?: Record<string, any>;
      };
    }>;
    statementType?: string;
    ddlAffectedRowAccessPolicyCount?: string;
    transferredBytes?: string;
    materializedViewStatistics?: {
      materializedView?: Array<{
        chosen?: boolean;
        estimatedBytesSaved?: string;
        rejectedReason?:
          | "REJECTED_REASON_UNSPECIFIED"
          | "NO_DATA"
          | "COST"
          | "BASE_TABLE_DATA_CHANGE"
          | "BASE_TABLE_PARTITION_EXPIRATION_CHANGE"
          | "BASE_TABLE_EXPIRED_PARTITION"
          | "BASE_TABLE_INCOMPATIBLE_METADATA_CHANGE"
          | "TIME_ZONE"
          | "OUT_OF_TIME_TRAVEL_WINDOW"
          | "BASE_TABLE_FINE_GRAINED_SECURITY_POLICY"
          | "BASE_TABLE_TOO_STALE";
        tableReference?: TableReference;
      }>;
    };
    metadataCacheStatistics?: {
      tableMetadataCacheUsage?: Array<{
        explanation?: string;
        tableReference?: TableReference;
        unusedReason?:
          | "UNUSED_REASON_UNSPECIFIED"
          | "EXCEEDED_MAX_STALENESS"
          | "METADATA_CACHING_NOT_ENABLED"
          | "OTHER_REASON";
      }>;
    };
  };
};

type JobStatus = {
  state?: "PENDING" | "RUNNING" | "DONE";
  errorResult?: ErrorProto;
  errors?: ErrorProto[];
};

type JobConfigurationQuery = {
  query: string;
  destinationTable?: TableReference;
  createDisposition?: "CREATE_IF_NEEDED" | "CREATE_NEVER";
  writeDisposition?: "WRITE_TRUNCATE" | "WRITE_APPEND" | "WRITE_EMPTY";
  defaultDataset?: {
    projectId: string;
    datasetId: string;
  };
  priority?: "INTERACTIVE" | "BATCH";
  preserveNulls?: boolean;
  allowLargeResults?: boolean;
  useQueryCache?: boolean;
  flattenResults?: boolean;
  maximumBillingTier?: number;
  maximumBytesBilled?: string;
  useLegacySql?: boolean;
  parameterMode?: "NAMED" | "POSITIONAL";
  queryParameters?: Array<{
    name?: string;
    parameterType: {
      type: string;
      arrayType?: any;
      structTypes?: Array<{
        name?: string;
        type?: any;
        description?: string;
      }>;
    };
    parameterValue: {
      value?: string;
      arrayValues?: any[];
      structValues?: Record<string, any>;
    };
  }>;
  schemaUpdateOptions?: Array<
    "ALLOW_FIELD_ADDITION" | "ALLOW_FIELD_RELAXATION"
  >;
  timePartitioning?: {
    type: "DAY" | "HOUR" | "MONTH" | "YEAR";
    expirationMs?: string;
    field?: string;
    requirePartitionFilter?: boolean;
  };
  rangePartitioning?: {
    field: string;
    range: {
      start: string;
      end: string;
      interval: string;
    };
  };
  clustering?: {
    fields: string[];
  };
  destinationEncryptionConfiguration?: {
    kmsKeyName: string;
  };
  scriptOptions?: {
    statementTimeoutMs?: string;
    statementByteBudget?: string;
    keyResultStatement?:
      | "KEY_RESULT_STATEMENT_KIND_UNSPECIFIED"
      | "LAST"
      | "FIRST_SELECT";
  };
  connectionProperties?: Array<{
    key: string;
    value: string;
  }>;
  createSession?: boolean;
  continuousMode?: boolean;
};

type JobConfigurationLoad = {
  sourceUris: string[];
  schema?: TableSchema;
  destinationTable: TableReference;
  createDisposition?: "CREATE_IF_NEEDED" | "CREATE_NEVER";
  writeDisposition?: "WRITE_TRUNCATE" | "WRITE_APPEND" | "WRITE_EMPTY";
  nullMarker?: string;
  fieldDelimiter?: string;
  skipLeadingRows?: number;
  encoding?: "UTF-8" | "ISO-8859-1";
  quote?: string;
  maxBadRecords?: number;
  allowQuotedNewlines?: boolean;
  sourceFormat?: "CSV" | "NEWLINE_DELIMITED_JSON" | "AVRO" | "DATASTORE_BACKUP" | "PARQUET" | "ORC";
  allowJaggedRows?: boolean;
  ignoreUnknownValues?: boolean;
  projectionFields?: string[];
  autodetect?: boolean;
  schemaUpdateOptions?: Array<
    "ALLOW_FIELD_ADDITION" | "ALLOW_FIELD_RELAXATION"
  >;
  timePartitioning?: {
    type: "DAY" | "HOUR" | "MONTH" | "YEAR";
    expirationMs?: string;
    field?: string;
    requirePartitionFilter?: boolean;
  };
  rangePartitioning?: {
    field: string;
    range: {
      start: string;
      end: string;
      interval: string;
    };
  };
  clustering?: {
    fields: string[];
  };
  destinationEncryptionConfiguration?: {
    kmsKeyName: string;
  };
  useAvroLogicalTypes?: boolean;
  parquetOptions?: {
    enumAsString?: boolean;
    enableListInference?: boolean;
  };
  hivePartitioningOptions?: {
    mode?: string;
    sourceUriPrefix?: string;
    requirePartitionFilter?: boolean;
    fields?: string[];
  };
  decimalTargetTypes?: Array<"NUMERIC" | "BIGNUMERIC" | "STRING">;
  jsonExtension?: "GEOJSON";
  connectionProperties?: Array<{
    key: string;
    value: string;
  }>;
  createSession?: boolean;
  columnNameCharacterMap?: "COLUMN_NAME_CHARACTER_MAP_UNSPECIFIED" | "STRICT" | "V1" | "V2";
  preserveAsciiControlCharacters?: boolean;
  referenceFileSchemaUri?: string;
};

type JobConfigurationExtract = {
  sourceTable?: TableReference;
  sourceModel?: {
    projectId: string;
    datasetId: string;
    modelId: string;
  };
  destinationUris: string[];
  printHeader?: boolean;
  fieldDelimiter?: string;
  destinationFormat?: "CSV" | "NEWLINE_DELIMITED_JSON" | "AVRO" | "PARQUET" | "ML_TF_SAVED_MODEL" | "ML_XGBOOST_BOOSTER";
  compression?: "GZIP" | "DEFLATE" | "SNAPPY" | "NONE";
  useAvroLogicalTypes?: boolean;
  modelExtractOptions?: {
    trialId?: string;
  };
};

type JobConfigurationCopy = {
  sourceTables?: TableReference[];
  sourceTable?: TableReference;
  destinationTable: TableReference;
  createDisposition?: "CREATE_IF_NEEDED" | "CREATE_NEVER";
  writeDisposition?: "WRITE_TRUNCATE" | "WRITE_APPEND" | "WRITE_EMPTY";
  destinationEncryptionConfiguration?: {
    kmsKeyName: string;
  };
  operationType?: "OPERATION_TYPE_UNSPECIFIED" | "COPY" | "SNAPSHOT" | "RESTORE" | "CLONE";
  destinationExpirationTime?: string;
};

type JobConfiguration = {
  jobType: "QUERY" | "LOAD" | "EXTRACT" | "COPY";
  query?: JobConfigurationQuery;
  load?: JobConfigurationLoad;
  extract?: JobConfigurationExtract;
  copy?: JobConfigurationCopy;
  dryRun?: boolean;
  jobTimeoutMs?: string;
  labels?: Record<string, string>;
};

type Job = {
  kind: string;
  etag: string;
  id: string;
  selfLink: string;
  user_email: string;
  configuration: JobConfiguration;
  jobReference: JobReference;
  statistics: JobStatistics;
  status: JobStatus;
  jobCreationReason?: {
    code:
      | "CODE_UNSPECIFIED"
      | "REQUESTED"
      | "LONG_RUNNING"
      | "LARGE_RESULTS"
      | "OTHER";
  };
};

type PostJobRequest = Omit<Job, "kind" | "etag" | "id" | "selfLink" | "statistics" | "status">;

export type {
  Job,
  PostJobRequest,
  JobReference,
  JobConfiguration,
  JobConfigurationQuery,
  JobConfigurationLoad,
  JobConfigurationExtract,
  JobConfigurationCopy,
  JobStatistics,
  JobStatus,
  ErrorProto,
};
