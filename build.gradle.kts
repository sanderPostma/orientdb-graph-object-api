plugins {
    kotlin("jvm") version "1.5.20"
    `maven-publish`
}
java.sourceCompatibility = JavaVersion.VERSION_11

group = "com.atomicvoid.orientdb"
version = "0.1.0"

repositories {
    mavenLocal()
    mavenCentral()
}


dependencies {
    implementation("com.orientechnologies:orientdb-server:3.1.12")
    implementation("com.orientechnologies:orientdb-studio:3.1.12")
    implementation("org.apache.commons:commons-lang3:3.12.0")
    implementation("net.oneandone.reflections8:reflections8:0.11.7")
    implementation("org.slf4j:slf4j-api:1.7.32")
}

tasks {
    withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> { kotlinOptions { jvmTarget = "1.8" } }
}

publishing {
    repositories {
        maven {
        }
    }
    publications {
        register("mavenJava", MavenPublication::class) {
            from(components["java"])
        }
    }
}

