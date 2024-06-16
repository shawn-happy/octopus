.PHONY: build

format:
	./gradlew clean spotlessApply

build:
	./gradlew clean spotlessCheck build

install:
	./gradlew clean spotlessApply build

deploy:
	./gradlew clean spotlessApply build