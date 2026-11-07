#! /bin/bash

if [ $1 == 'opa' ]
then
  opa test opa/trino -v

elif [ $1 == 'bundle' ]
then
  mkdir -p tmp
  cd tmp
  wget --header="Authorization: Bearer bearer-token" localhost:8000/api/v1/opa/bundle/trino -O trino.tar.gz
  tar -zxvf trino.tar.gz
  rm trino.tar.gz
  ls
elif [ $1 == 'format' ]
then
  autoflake --in-place --remove-unused-variables -r moat
elif [ $1 == 'unit' ]
then
  export CONFIG_FILE_PATH=moat/config/config.unittest.yaml
  export PYTHON
  cd moat
  python -m pytest
elif [ "$1" == 'policy-evaluator' ]
then
  podman-compose run --rm --no-deps --build \
    -e CONFIG_FILE_PATH=config/config.docker.yaml \
    --entrypoint python moat \
    -m cli.src.cli evaluate-policy
elif [ $1 == 'scim' ]
then
  scim -u http://localhost:8000/api/scim/v2 --header "Authorization: Bearer scim-token" test --verbose
fi
