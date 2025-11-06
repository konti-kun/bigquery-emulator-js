/**
 * Convert BigQuery format string to date-fns format string
 * BigQuery uses strftime-style format codes
 */
export function convertBigQueryFormatToDateFns(bigQueryFormat: string): string {
  let result = bigQueryFormat;

  // The basic format codes are mostly compatible:
  // %Y -> yyyy (4-digit year)
  // %m -> MM (2-digit month)
  // %d -> dd (2-digit day)
  // %H -> HH (2-digit hour 00-23)
  // %M -> mm (2-digit minute)
  // %S -> ss (2-digit second)

  result = result.replace(/%Y/g, "yyyy");
  result = result.replace(/%m/g, "MM");
  result = result.replace(/%d/g, "dd");
  result = result.replace(/%H/g, "HH");
  result = result.replace(/%M/g, "mm");
  result = result.replace(/%S/g, "ss");
  result = result.replace(/T/g, "\'T\'");

  return result;
}
