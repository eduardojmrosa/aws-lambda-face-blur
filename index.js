const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { RekognitionClient, DetectFacesCommand } = require('@aws-sdk/client-rekognition');
const Jimp = require('jimp');

const rekognition = new RekognitionClient({ region: "us-east-1" });
const s3na = new S3Client({ region: "us-east-1" });
const s3sa = new S3Client({ region: "sa-east-1" });

const srcBucket = "origin-bucket-name";
const destBucket = "destination-bucket-name";

/**
 * Lambda function to apply blur effect on detected faces in images received in S3 bucket.
 * @param {Object} event - The Lambda event that triggers the function. It should contain information about the object created in S3.
 * @returns {Promise<Buffer>} - Returns a buffer of the blurred image.
 */
exports.handler = async (event) => {
  try {
    let srcKey;
    if (event.Records && event.Records.length > 0) {
      srcKey = event.Records[0].s3.object.key;
    }

    if (!srcKey) {
      throw new Error("No records received for reading.");
    }

    const response = await downloadImage(srcKey);
    const faceDetails = await detectFaces(response);
    const buffer = await blurFaces(response.Body, faceDetails);
    const uploadSucess = await uploadImage(buffer, srcKey)
    return uploadSucess;
  } catch (error) {
    console.error(error);
    throw error;
  }
};

/**
 * Downloads the image from S3.
 * @param {string} srcKey - The key of the object in S3.
 * @returns {Promise<{Body: Buffer}>} - Returns an object containing the image buffer.
 */
const downloadImage = async (srcKey) => {
  const command = new GetObjectCommand({
    Bucket: srcBucket,
    Key: srcKey
  });

  try {
    const response = await s3na.send(command);
    const body = await streamToBuffer(response.Body);
    return { Body: body };
  } catch (error) {
    throw error;
  }
};

/**
 * Detects faces in the image using Rekognition.
 * @param {Object} response - Response from image download.
 * @returns {Promise<Array>} - Returns an array of detected face details.
 */
const detectFaces = async (response) => {
  const params = {
    Image: { Bytes: response.Body }, // Passing image bytes to Rekognition
    Attributes: ['DEFAULT']
  };
  const command = new DetectFacesCommand(params);

  try {
    const data = await rekognition.send(command);
    return data.FaceDetails;
  } catch (error) {
    throw error;
  }
};

/**
 * Applies the blur effect on detected faces in the image.
 * @param {Buffer} imageBuffer - Image buffer.
 * @param {Array} faceDetails - Detected face details.
 * @returns {Promise<Buffer>} - Returns a buffer of the blurred image.
 */

const blurFaces = async (imageBuffer, faceDetails) => {
  try {
    const image = await Jimp.read(imageBuffer);
    const metadata = image.bitmap;

    faceDetails.forEach(faceDetail => {
      const box = faceDetail.BoundingBox;
      const left = Math.floor(box.Left * metadata.width);
      const top = Math.floor(box.Top * metadata.height);
      const width = Math.floor(box.Width * metadata.width);
      const height = Math.floor(box.Height * metadata.height);

      // Adjusting coordinates to ensure distortion stays within image bounds
      const x = Math.max(left - 10, 0);
      const y = Math.max(top - 10, 0);
      const w = Math.min(width + 20, metadata.width - x);
      const h = Math.min(height + 20, metadata.height - y);

      // Applying a distortion effect on the face
      image.scan(x, y, w, h, function (x, y, idx) {
        // Adding some distortion to pixel values
        const distortion = Math.random() * 200; // adjust distortion value as needed
        this.bitmap.data[idx + 0] += distortion; // Red
        this.bitmap.data[idx + 1] += distortion; // Green
        this.bitmap.data[idx + 2] += distortion; // Blue
      });
    });

    // Save the resulting image to a buffer
    const buffer = await image.getBufferAsync(Jimp.MIME_JPEG);
    return buffer;
  } catch (error) {
    throw error;
  }
};



/**
 * Uploads the blurred image to the destination S3 bucket.
 * @param {Buffer} buffer - Blurred image buffer.
 * @param {string} srcKey - Key of the object in S3.
 * @returns {Promise<void>}
 */
const uploadImage = async (buffer, srcKey) => {
  const command = new PutObjectCommand({
    Bucket: destBucket,
    Key: srcKey,
    Body: buffer,
    ContentType: "image/jpeg", // Assuming images are JPEG
    ACL: 'public-read'
  });

  try {
    await s3sa.send(command);
  } catch (error) {
    throw error;
  }
};

/**
 * Converts a stream to a buffer.
 * @param {stream.Readable} stream - Stream to be converted.
 * @returns {Promise<Buffer>} - Returns a buffer.
 */
const streamToBuffer = async (stream) => {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
};
