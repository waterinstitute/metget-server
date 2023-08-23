![MetGet Logo](https://raw.githubusercontent.com/waterinstitute/metget-server/main/img/MetGet_logo_blue.png)

MetGet is an application which allows users to query, format, and blend meteorological data from various sources
to be used in hydrodynamic modeling applications. The application is deployed using Kubernetes and
is configured to run as a set of services within the cluster acting as a RESTful API.

This project is in active development and should not be considered a production level product at this time.

## Development Partners

MetGet is developed by [The Water Institute](https://thewaterinstitute.org) and has been funded by the 
University of North Carolina at Chapel Hill [Coastal Resilience Center of Excellence](https://www.coastalresiliencecenter.org).

![The Water Institute](https://thewaterinstitute.org/images/01-Primary_Logo_Final.png)

![Coastal Resilience Center of Excellence](https://coastalresiliencecenter.unc.edu/wp-content/uploads/sites/845/2019/01/CoastalResilienceCenter-notexture-horiz-DHS-large.png)

![University of North Carolina at Chapel Hill](https://identity.unc.edu/wp-content/uploads/sites/885/2019/01/UNC_logo_webblue-e1517942350314.png)

## Recent Applications

MetGet has been utilized in forecasting applications, including those utilized during Hurricane Ian (2022)
where MetGet was able to supply multiple forecasting groups with meteorological data from the National Hurricane Center,
Global Forecast System, Hurricane Weather Research and Forecasting Model (HWRF), and COAMPS-TC model from the Naval Research
Laboratory in real time.

## Dependencies

MetGet is written in Python and C++ and utilizes the following applications/libraries:
- Kubernetes
- Helm
- Argo
- Postgres
- RabbitMQ
- Flask
- ecCodes
- netCDF4

## Usage

The `metget-server` should be deployed to a Kubernetes cluster using Helm. 
The Helm chart is located in the `helm` directory.

The `metget` client application is a Python package which interacts with the `metget-server` can be 
installed using `pip`:

```bash
pip install metget
```

The `metget` client application can be used to query the `metget-server` for meteorological data. 

## Installation

The installation assumes that:

1. You have an operational Kubernetes cluster
2. You have installed Helm
3. You have installed Argo Workflows and Argo Events
4. You have established an ingress to view the Argo dashboard

If you have not done so, please follow the instructions for installing the above applications.

### Required AWS Services

The `metget-server` uses S3 buckets to store both meteorological source data and the output from client
requests. The user will need to create two S3 buckets for use with the `metget-server` and specify them 
during the configuration steps. It is recommended that the bucket used for internal storage of meteorological
data be set as private and the bucket used for client requests be set as public without list permissions. The
user should also create an IAM user with access to both buckets and specify the access key and secret key
during the configuration steps. Lastly, it is recommended that you set a lifecycle policy on the bucket used
for the output of client requests to delete objects after a certain period of time. We typically use 3 days
which is more than enough time for the user to download the data, but it can be made as long or short as you
like.

A second AWS service which may be optionally used is a load balancer. This is used to map the `metget-server`
ingress to a domain name. This is not required, but is recommended for ease of use, particularly if you are
using EKS. This will be specified in the `http` section of the `values.yaml` file.

### Install the MetGet Helm Chart

The `metget-server` is deployed to a Kubernetes cluster using Helm. To install the Helm chart, you will
need navigate to the `helm/metget-server` and copy the `values.example.yaml` file to `values.yaml`:

```bash
cd helm/metget-server
cp values.example.yaml values.yaml
```

Then, edit the `values.yaml` file to reflect your environment. By default, the  `values.yaml` file reflects the
stable container images for use within the system. However, there are also nightly and development images available
which may be of interest to some users.

Additionally, users may want to turn on/off various types of meteorology. Certain types of meteorology which cannot
be reliably sourced via Amazon's Big Data Service (e.g. HWRF, WPC, COAMPS-TC) will incur additional costs. Users 
should be aware of these costs before turning on these types of meteorology.

Second, if more than one `metget-server` instance is installed (i.e. development and production), the S3 
buckets that it points to for its own storage should be unique. If they are not unique, you will encounter 
undefined behavior. 

Once you've gone through the various configuration options, you can install the Helm chart. From the `helm`
directory, run:

```bash
helm install [installation-name] ./metget-server --namespace [namespace]
```

Where `[installation-name]` is the name you'd like to give the installation and `[namespace]` is the namespace
you'd like to install the application to. 

:warning: WARNING: Each `metget-server` installation should be installed to its own namespace.

