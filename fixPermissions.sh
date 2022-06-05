#!/bin/sh

chmod -R 775 "/kcc_prd/ykdl/jobs"
chmod -R 770 "/kcc_prd/ykdl/data"
chown -R "root:had_kcc_ykdl_rw_p" "/kcc_prd/ykdl/jobs"
chown -R "root:had_kcc_ykdl_rw_p" "/kcc_prd/ykdl/data"

