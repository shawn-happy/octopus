# Spark-On-K8S-Operators安装手册

## 安装minikube

到 https://github.com/kubernetes/minikube/releases 下载 minikube，我安装的是 minikube v1.30.1。

下载完成后修改文件名为 `minikube`，然后 `chmod +x minikube`，移动到 `$PATH` 目录下：

```bash
sudo mv ~/Download/minikube-darwin-arm64 /usr/local/bin/
sudo chmod +x /usr/local/bin/minikube
```

启动minikube

```bash
minikube start
```

## 安装Helm

到https://github.com/helm/helm/releases下载Helm，我安装的是v3.11.3。

```bash
tar -zxvf helm-v3.10.1-darwin-arm64.tar.gz
mv darwin-arm64/helm /usr/local/bin/helm
```

## 安装Spark-On-K8S-Operators

1. 添加源

   ```bash
   helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
   ```

2. helm安装Spark-On-K8S-Operators

   ```bash
   helm install spark-operator spark-operator/spark-operator --namespace operators --create-namespace --set sparkJobNamespace=operators
   ```

kubectl get secret argo-server-token-xkcpr -n operators -o=jsonpath='{.data.token}' | base64 --decode