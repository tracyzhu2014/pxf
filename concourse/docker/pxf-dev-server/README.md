# Build the pxf-dev-server docker images locally

Build the docker images on your local system. To build the following docker
images you need the `singlecluster` tarball for the Hadoop flavor you plan to build:

### Docker gpdb6-centos7-test-pxf-cdh-image

Build this image for Greenplum 6 running on CentOS 7 with Cloudera support.
Run the following command to build the image:

```
docker build \
  --build-arg=BASE_IMAGE=gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest \
  --build-arg=SINGLE_CLUSTER_TARBALL=singlecluster-CDH.tar.gz \
  --tag=gpdb6-centos7-test-pxf-cdh \
  -f ~/workspace/pxf/concourse/docker/pxf-dev-server/Dockerfile .
```

### Docker gpdb6-centos7-test-pxf-hdp2-image

Build this image for Greenplum 6 running on CentOS 7 with HDP 2 support.
Run the following command to build the image:

```
docker build \
  --build-arg=BASE_IMAGE=gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest \
  --build-arg=SINGLE_CLUSTER_TARBALL=singlecluster-HDP2.tar.gz \
  --tag=gpdb6-centos7-test-pxf-hdp2 \
  -f ~/workspace/pxf/concourse/docker/pxf-dev-server/Dockerfile .
```

### Docker gpdb6-centos7-test-pxf-hdp3-image

Build this image for Greenplum 6 running on CentOS 7 with HDP 3 support.
Run the following command to build the image:

```
docker build \
  --build-arg=BASE_IMAGE=gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest \
  --build-arg=SINGLE_CLUSTER_TARBALL=singlecluster-HDP3.tar.gz \
  --tag=gpdb6-centos7-test-pxf-hdp3 \
  -f ~/workspace/pxf/concourse/docker/pxf-dev-server/Dockerfile .
```

More images to come...