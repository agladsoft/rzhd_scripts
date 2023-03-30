#!/bin/bash

xls_path="${XL_IDP_PATH_RZHD}/rzhd_petersburg"

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

  echo "Will convert XLS* '${file}' to JSON '${json_path}'"
  exit_message=$(python3 "${XL_IDP_ROOT_RZHD}/scripts/rzhd.py" "${file}" "${json_path}" 2>&1 > /dev/null)

  exit_code=$?
  echo "Exit code ${exit_code}"
  if [[ ${exit_code} == 0 ]]
	then
	  mv "${file}" "${done_path}"
	else
    for error_code in {1..6}
    do
      if [[ ${exit_code} == "${error_code}" ]]
      then
        mv "${file}" "${xls_path}/error_${exit_message}_$(basename "${file}")"
      fi
    done
	fi

done