package org.greenplum.pxf.automation.features.image;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HdfsReadableImageTest extends BaseFeature {
    private static final int NUM_IMAGES = 5;
    private String hdfsPath;
    private File[] imageFiles;
    private StringBuilder[] imagesPostgresArrays;
    private Table compareTable;
    private String[] fullPaths;
    private String[] directories;
    private String[] names;
    private ProtocolEnum protocol;

    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
        protocol = ProtocolUtils.getProtocol();
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/readableImage";
        prepareData();
    }

    private void prepareData() throws Exception {
        BufferedImage[] bufferedImages = new BufferedImage[NUM_IMAGES];
        imageFiles = new File[NUM_IMAGES];
        fullPaths = new String[NUM_IMAGES];
        directories = new String[NUM_IMAGES];
        names = new String[NUM_IMAGES];
        int w = 256;
        int h = 128;
        for (int i = 0; i < NUM_IMAGES; i++) {
            bufferedImages[i] = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        }
        imagesPostgresArrays = new StringBuilder[NUM_IMAGES];
        appendToImages(imagesPostgresArrays, "{{", 0);

        Map<String, Integer> maskOffsets = new HashMap<String, Integer>() {{
            put("r", 16);
            put("g", 8);
            put("b", 0);
        }};

        Random rand = new Random();
        for (int j = 0; j < NUM_IMAGES; j++) {
            for (int i = 0; i < w * h; i++) {
                final int r = rand.nextInt(256) << maskOffsets.get("r");
                final int g = rand.nextInt(256) << maskOffsets.get("g");
                final int b = rand.nextInt(256) << maskOffsets.get("b");
                bufferedImages[j].setRGB(i % w, i / w, r + g + b);
                imagesPostgresArrays[j]
                        .append("{")
                        .append(r >> maskOffsets.get("r"))
                        .append(",")
                        .append(g >> maskOffsets.get("g"))
                        .append(",")
                        .append(b >> maskOffsets.get("b"))
                        .append("},");
                if ((i + 1) % w == 0) {
                    appendToImage(imagesPostgresArrays[j], "},{", 1);
                }
            }
        }
        appendToImages(imagesPostgresArrays, "}", 2);

        String publicStage = "/tmp/publicstage/pxf";
        createDirectory(publicStage);

        int cnt = 0;
        for (BufferedImage bi : bufferedImages) {
            names[cnt] = String.format("%d.png", cnt);
            imageFiles[cnt] = new File(publicStage + "/" + names[cnt]);
            ImageIO.write(bi, "png", imageFiles[cnt]);
            fullPaths[cnt] = (protocol != ProtocolEnum.HDFS ? hdfsPath.replaceFirst("[^/]*/", "/") : "/" + hdfsPath) + "/" + names[cnt];
            hdfs.copyFromLocal(imageFiles[cnt].toString(), hdfsPath + "/" + names[cnt]);
            hdfs.copyFromLocal(imageFiles[cnt].toString(), hdfsPath + "_extra_dir/" + names[cnt]);
            directories[cnt] = "readableImage";
            cnt++;
        }
    }

    private void createDirectory(String dir) {
        File publicStageDir = new File(dir);
        if (!publicStageDir.exists()) {
            if (!publicStageDir.mkdirs()) {
                throw new RuntimeException(String.format("Could not create %s", dir));
            }
        }
    }

    private void appendToImages(StringBuilder[] sbs, String s, int offsetFromEnd) {
        if (sbs[0] == null) {
            for (int i = 0; i < sbs.length; i++) {
                sbs[i] = new StringBuilder();
            }
        }
        for (StringBuilder sb : sbs) {
            appendToImage(sb, s, offsetFromEnd);
        }
    }

    private void appendToImage(StringBuilder sb, String s, int offsetFromEnd) {
        if (sb == null) {
            sb = new StringBuilder();
        }
        sb.setLength(sb.length() - offsetFromEnd);
        sb.append(s);
    }

    @Override
    public void beforeMethod() throws Exception {
        // default external table with common settings
        exTable = new ReadableExternalTable("image_test", null, "", "CSV");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        final String[] imageTableFields = new String[]{
                "fullpaths TEXT[]",
                "directories TEXT[]",
                "names TEXT[]",
                "images INT[]"
        };
        exTable.setFields(imageTableFields);
        exTable.setPath(hdfsPath + "/*.png");
        exTable.setProfile(protocol.value() + ":image");
        compareTable = new Table("compare_table", imageTableFields);
        compareTable.setRandomDistribution();
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void batchSize1() throws Exception {
        exTable.setName("image_test_batchsize_1");
        compareTable.setName("compare_table_batchsize_1");
        int cnt = 0;
        for (StringBuilder image : imagesPostgresArrays) {
            compareTable.addRow(new String[]{
                    "'{" + fullPaths[cnt] + "}'",
                    "'{" + directories[cnt] + "}'",
                    "'{" + names[cnt] + "}'",
                    "'{" + image + "}'"
            });
            cnt++;
        }

        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable);
        gpdb.runQuery(compareTable.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.batchsize_1.runTest");
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void smallBatchSize() throws Exception {
        exTable.setName("image_test_small_batchsize");
        exTable.setUserParameters(new String[]{"BATCH_SIZE=3"});
        compareTable.setName("compare_table_small_batchsize");
        compareTable.addRow(new String[]{
                "'{" + fullPaths[0] + "," + fullPaths[1] + "," + fullPaths[2] + "}'",
                "'{" + directories[0] + "," + directories[1] + "," + directories[2] + "}'",
                "'{" + names[0] + "," + names[1] + "," + names[2] + "}'",
                "'{" + imagesPostgresArrays[0] + "," + imagesPostgresArrays[1] + "," + imagesPostgresArrays[2] + "}'"
        });
        compareTable.addRow(new String[]{
                "'{" + fullPaths[3] + "," + fullPaths[4] + "}'",
                "'{" + directories[3] + "," + directories[4] + "}'",
                "'{" + names[3] + "," + names[4] + "}'",
                "'{" + imagesPostgresArrays[3] + "," + imagesPostgresArrays[4] + "}'"
        });

        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable);
        gpdb.runQuery(compareTable.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.small_batchsize.runTest");
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void largeBatchSize() throws Exception {
        exTable.setName("image_test_large_batchsize");
        exTable.setUserParameters(new String[]{"BATCH_SIZE=10"});
        compareTable.setName("compare_table_large_batchsize");
        compareTable.addRow(new String[]{
                "'{" + fullPaths[0] + "," + fullPaths[1] + "," + fullPaths[2] + "," + fullPaths[3] + "," + fullPaths[4] + "}'",
                "'{" + directories[0] + "," + directories[1] + "," + directories[2] + "," + directories[3] + "," + directories[4] + "}'",
                "'{" + names[0] + "," + names[1] + "," + names[2] + "," + names[3] + "," + names[4] + "}'",
                "'{" + imagesPostgresArrays[0] + "," + imagesPostgresArrays[1] + "," + imagesPostgresArrays[2] + "," + imagesPostgresArrays[3] + "," + imagesPostgresArrays[4] + "}'"
        });

        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable);
        gpdb.runQuery(compareTable.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.large_batchsize.runTest");
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void filesInDifferentDirectories() throws Exception {
        // this test ensures that the different directories yield different
        // image 'labels'
        exTable.setName("image_test_images_in_different_directories");
        compareTable.setName("compare_table_images_in_different_directories");
        int cnt = 0;
        for (StringBuilder image : imagesPostgresArrays) {
            compareTable.addRow(new String[]{
                    "'{" + fullPaths[cnt] + "}'",
                    "'{" + directories[cnt] + "}'",
                    "'{" + names[cnt] + "}'",
                    "'{" + image + "}'"
            });
            compareTable.addRow(new String[]{
                    "'{" + fullPaths[cnt].replace("readableImage", "readableImage_extra_dir") + "}'",
                    "'{" + directories[cnt] + "_extra_dir}'", // different directory reflected in image 'label'
                    "'{" + names[cnt] + "}'",
                    "'{" + image + "}'"
            });
            cnt++;
        }

        // open up path to include extra directory
        exTable.setPath(hdfsPath + "*/*.png");
        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable);
        gpdb.runQuery(compareTable.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.images_in_different_directories.runTest");
    }


    @Override
    public void afterClass() throws Exception {
        super.afterMethod();
        if (ProtocolUtils.getPxfTestDebug().equals("true")) {
            return;
        }
        for (File fileToDelete : imageFiles) {
            if (!fileToDelete.delete()) {
                throw new RuntimeException(String.format("Could not delete %s", fileToDelete));
            }
        }
    }
}