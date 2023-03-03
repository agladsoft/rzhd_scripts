#!/bin/bash

xls_path="${XL_IDP_PATH_RZHD}/rzhd_weekly"

csv_path="${xls_path}"/csv
if [ ! -d "$csv_path" ]; then
  mkdir "${csv_path}"
fi

done_path="${xls_path}"/done
if [ ! -d "$done_path" ]; then
  mkdir "${done_path}"
fi

json_path="${xls_path}"/json
if [ ! -d "$json_path" ]; then
  mkdir "${json_path}"
fi

find "${xls_path}" -maxdepth 1 -type f \( -name "*.xls*" \) ! -newermt '3 seconds ago' -print0 | while read -d $'\0' file
do

  if [[ "${file}" == *"error_"* ]];
  then
    continue
  fi

	mime_type=$(file -b --mime-type "$file")
  echo "'${file} - ${mime_type}'"

  csv_name="${csv_path}/$(basename "${file}").csv"

  if [[ ${mime_type} = "application/vnd.ms-excel" ]]
  then
    echo "Will convert XLS '${file}' to CSV '${csv_name}'"
    xlsx2csv "${file}" > "${csv_name}"
  elif [[ ${mime_type} = "application/octet-stream" ]]
  then
    echo "Will convert XLSX or XLSM '${file}' to CSV '${csv_name}'"
    xlsx2csv "${file}" > "${csv_name}"
  elif [[ ${mime_type} = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ]]
  then
    echo "Will convert XLSX or XLSM '${file}' to CSV '${csv_name}'"
    xlsx2csv "${file}" > "${csv_name}"
  else
    echo "ERROR: unsupported format ${mime_type}"
    mv "${file}" "${xls_path}/error_$(basename "${file}")"
    continue
  fi

  if [ $? -eq 0 ]
	then
	  mv "${file}" "${done_path}"
	else
	  echo "ERROR during convertion ${file} to csv!"
	  mv "${file}" "${xls_path}/error_$(basename "${file}")"
	  continue
	fi

  python3 ${XL_IDP_ROOT_RZHD}/scripts/rzhd.py "${csv_name}" "${json_path}"

  if [ $? -eq 0 ]
	then
	  mv "${csv_name}" "${done_path}"
	else
	  echo "ERROR during convertion ${file} to json!"
	  mv "${csv_name}" "${xls_path}/error_$(basename "${csv_name}")"
	  continue
	fi

done