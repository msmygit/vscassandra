/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.carrotsearch.randomizedtesting.rules.StatementAdapter;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.apache.cassandra.index.sai.SAITester.EMPTY_COMPARATOR;

public class LatestVersionManager implements TestRule
{
    @Override
    public Statement apply(Statement statement, Description description)
    {
        if (isVersionRequired(description))
        {
            return new StatementAdapter(statement)
            {
                Version requiredVersion;
                Version originalVersion;

                @Override
                protected void before() throws Throwable
                {
                    requiredVersion = Version.parse(getAnnotation(description, RequiresVersion.class).version());
                    originalVersion = Version.LATEST;
                    setLatestVersion(requiredVersion);
                    setTestFactory(requiredVersion);
                }

                @Override
                protected void afterAlways(List<Throwable> errors) throws Throwable
                {
                    setLatestVersion(originalVersion);
                    setTestFactory(originalVersion);
                }
            };
        }
        return statement;
    }

    private boolean isVersionRequired(Description description)
    {
        return getAnnotation(description, RequiresVersion.class) != null;
    }

    private RequiresVersion getAnnotation(Description description, Class<? extends Annotation> annotationType)
    {
        Annotation annotation = description.getAnnotation(annotationType);
        if (annotation != null)
            return (RequiresVersion) annotation;

        annotation = description.getTestClass().getAnnotation(annotationType);
        if (annotation != null)
            return (RequiresVersion) annotation;

        return null;
    }

    public static void setLatestVersion(Version version) throws Throwable
    {
        Field latest = Version.class.getDeclaredField("LATEST");
        latest.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(latest, latest.getModifiers() & ~Modifier.FINAL);
        latest.set(null, version);
    }

    public static void setTestFactory(Version version) throws Throwable
    {
        Field latest = SAITester.class.getDeclaredField("TEST_FACTORY");
        latest.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(latest, latest.getModifiers() & ~Modifier.FINAL);
        latest.set(null, version.onDiskFormat().primaryKeyFactory(EMPTY_COMPARATOR));
    }
}
