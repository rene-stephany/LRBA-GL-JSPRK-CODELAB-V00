package com.bbva.lrba.gl.jsprk.codelab.v00;

import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobCodelabBuilderTest {

    private JobCodelabBuilder jobCodelabBuilder;

    @BeforeEach
    void setUp() {
        this.jobCodelabBuilder = new JobCodelabBuilder();
    }

    @Test
    void registerSources_na_SourceList() {
        final SourcesList sourcesList = this.jobCodelabBuilder.registerSources();
        assertNotNull(sourcesList);
        assertNotNull(sourcesList.getSources());
        assertEquals(3, sourcesList.getSources().size());

        final Source source = sourcesList.getSources().get(0);
        assertNotNull(source);
        assertEquals("sourceAlias1", source.getAlias());
        assertEquals("input1.csv", source.getPhysicalName());

        final Source source2 = sourcesList.getSources().get(1);
        assertNotNull(source2);
        assertEquals("sourceAlias2", source2.getAlias());
        assertEquals("input2.csv", source2.getPhysicalName());

        final Source source3 = sourcesList.getSources().get(2);
        assertNotNull(source3);
        assertEquals("sourceAlias3", source3.getAlias());
        assertEquals("input3.csv", source3.getPhysicalName());
    }

    @Test
    void registerTransform_na_Transform() {
        //IF YOU WANT TRANSFORM CLASS
        final TransformConfig transformConfig = this.jobCodelabBuilder.registerTransform();
        assertNotNull(transformConfig);
        assertNotNull(transformConfig.getTransform());
        //IF YOU WANT SQL TRANSFORM
        //final TransformConfig transformConfig = this.jobCodelabBuilder.registerTransform();
        //assertNotNull(transformConfig);
        //assertNotNull(transformConfig.getTransformSqls());
        //IF YOU DO NOT WANT TRANSFORM
        //final TransformConfig transformConfig = this.jobCodelabBuilder.registerTransform();
        //assertNull(transformConfig);
    }

    @Test
    void registerTargets_na_TargetList() {
        final TargetsList targetsList = this.jobCodelabBuilder.registerTargets();
        assertNotNull(targetsList);
        assertNotNull(targetsList.getTargets());
        assertEquals(1, targetsList.getTargets().size());

        final Target target = targetsList.getTargets().get(0);
        assertNotNull(target);
        assertEquals("targetAlias1", target.getAlias());
        assertEquals("output/output.csv", target.getPhysicalName());

    }

}