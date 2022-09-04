package s3rpccreate

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/bep/awscreate"
)

// New creates a new provisioner for the setup in https://github.com/bep/s3rpc
// This is probably not generally useful.
func New[T CreateResults](opts Options) awscreate.Provisioner[T] {
	adminCfg := opts.AdminCfg

	var (
		name           = opts.Name
		userNameClient = fmt.Sprintf("%s_client", name)
		userNameServer = fmt.Sprintf("%s_server", name)

		// queueClient is where the client listens for new messages.
		queueClient = userNameClient

		// queueServer is where the server listens for new messages.
		queueServer = userNameServer

		attrs = map[string]string{
			"MessageRetentionPeriod":        "7200", // 2 hours
			"ReceiveMessageWaitTimeSeconds": "10",   // 10 seconds
		}
	)

	inputs := createInputs{
		Name:     name,
		UserPath: "/",
		Queues: []*sqs.CreateQueueInput{
			{
				QueueName:  &queueClient,
				Attributes: attrs,
			},
			{
				QueueName:  &queueServer,
				Attributes: attrs,
			},
		},
		Users: []*iam.CreateUserInput{
			{UserName: &userNameClient},
			{UserName: &userNameServer},
		},
		Buckets: []*s3.CreateBucketInput{
			{
				Bucket: &name,
				CreateBucketConfiguration: &types.CreateBucketConfiguration{
					LocationConstraint: types.BucketLocationConstraint(opts.Region),
				},
			},
		},
		BucketsNotifications: []*s3.PutBucketNotificationConfigurationInput{
			{
				Bucket: &name,
				NotificationConfiguration: &types.NotificationConfiguration{
					QueueConfigurations: []types.QueueConfiguration{
						{
							Id: aws.String("To Client"),
							Events: []types.Event{
								"s3:ObjectCreated:*",
							},
							Filter: &types.NotificationConfigurationFilter{
								Key: &types.S3KeyFilter{
									FilterRules: []types.FilterRule{
										{
											Name:  "prefix",
											Value: aws.String("to_client/"),
										},
									},
								},
							},
							// QueueArn is set later.
						},
						{
							Id: aws.String("To Server"),
							Events: []types.Event{
								"s3:ObjectCreated:*",
							},
							Filter: &types.NotificationConfigurationFilter{
								Key: &types.S3KeyFilter{
									FilterRules: []types.FilterRule{
										{
											Name:  "prefix",
											Value: aws.String("to_server/"),
										},
									},
								},
							},
							// QueueArn is set later.
						},
					},
				},
			},
		},
	}

	return awscreate.NewProvisioner(
		func(ctx context.Context) (T, error) {
			v, err := create(ctx, adminCfg, inputs)
			return T(v), err
		},
		func(ctx context.Context) error {
			return destroy(ctx, adminCfg, inputs)
		},
	)
}

type Options struct {
	Name   string
	Region string

	AdminCfg aws.Config
}

type createInputs struct {
	Name     string
	UserPath string

	Users                []*iam.CreateUserInput
	Queues               []*sqs.CreateQueueInput
	Buckets              []*s3.CreateBucketInput
	BucketsNotifications []*s3.PutBucketNotificationConfigurationInput
}

type CreateResults struct {
	AccessKeys           []*iam.CreateAccessKeyOutput
	Queues               []*sqs.CreateQueueOutput
	Buckets              []*s3.CreateBucketOutput
	BucketsNotifications []*s3.PutBucketNotificationConfigurationOutput
}

func create(ctx context.Context, adminCfg aws.Config, inputs createInputs) (CreateResults, error) {
	var res CreateResults
	iamClient := iam.NewFromConfig(adminCfg)
	adminAccount, err := iamClient.GetUser(ctx, &iam.GetUserInput{})
	if err != nil {
		return res, err
	}

	arn := *adminAccount.User.Arn
	accountID := strings.Split(arn, ":")[4]
	var createdUsers []*iam.CreateUserOutput

	queueArn := func(name string) string {
		return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", adminCfg.Region, accountID, name)
	}

	if len(inputs.Users) > 0 {
		createdUsers = make([]*iam.CreateUserOutput, len(inputs.Users))
		for i, userInput := range inputs.Users {
			userInput.Path = &inputs.UserPath
			u, err := iamClient.CreateUser(ctx, userInput)
			if err != nil {
				return res, fmt.Errorf("failed to create user: %w", err)
			}
			createdUsers[i] = u
			a, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: u.User.UserName})
			if err != nil {
				return res, fmt.Errorf("failed to create access key: %w", err)
			}
			res.AccessKeys = append(res.AccessKeys, a)
		}
	}

	var awsPrincipals []string
	for _, user := range createdUsers {
		awsPrincipals = append(awsPrincipals, *user.User.Arn)
	}

	if len(inputs.Queues) > 0 {
		sqsClient := sqs.NewFromConfig(adminCfg)
		time.Sleep(61 * time.Second)

		for _, queueInput := range inputs.Queues {

			q, err := sqsClient.CreateQueue(ctx, queueInput)
			if err != nil {
				return res, fmt.Errorf("failed to create queue: %w", err)
			}
			res.Queues = append(res.Queues, q)
		}

		policy := awscreate.PolicyDocumentResource{
			Version: "2012-10-17",
			Statement: []awscreate.PolicyDocumentResourceStatementEntry{
				{
					Effect: "Allow",
					Action: []string{
						"sqs:*",
					},
				},
			},
		}

		policy.Statement[0].Principal = map[string]any{
			"AWS":     awsPrincipals,
			"Service": "s3.amazonaws.com",
		}

		for i, q := range res.Queues {
			policy.Statement[0].Resource = []string{fmt.Sprintf("arn:aws:sqs:%s:%s:%s", adminCfg.Region, accountID, *inputs.Queues[i].QueueName)}
			b, err := json.Marshal(policy)
			if err != nil {
				return res, err
			}

			s := string(b)

			_, err = sqsClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
				QueueUrl: q.QueueUrl,
				Attributes: map[string]string{
					"Policy": s,
				},
			})
			if err != nil {
				return res, fmt.Errorf("failed to set queue policy: %w", err)
			}
		}

	}

	if len(inputs.Buckets) > 0 {
		s3Client := s3.NewFromConfig(adminCfg)

		for _, bucketInput := range inputs.Buckets {
			b, err := s3Client.CreateBucket(ctx, bucketInput)
			if err != nil {
				return res, fmt.Errorf("failed to create bucket: %w", err)
			}
			fmt.Println("Created bucket", *bucketInput.Bucket, err)
			res.Buckets = append(res.Buckets, b)
		}

		for _, bucketInput := range inputs.Buckets {
			_, err := s3Client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
				Bucket: bucketInput.Bucket,

				LifecycleConfiguration: &types.BucketLifecycleConfiguration{
					Rules: []types.LifecycleRule{
						{
							ID: aws.String("Expire all after 1 day"),
							Filter: &types.LifecycleRuleFilterMemberPrefix{
								Value: "",
							},
							Status: types.ExpirationStatusEnabled,
							Expiration: &types.LifecycleExpiration{
								Days: 1,
							},
						},
					},
				},
			})

			if err != nil {
				return res, fmt.Errorf("failed to set bucket lifecycle: %w", err)
			}
		}

		policy := awscreate.PolicyDocumentResource{
			Version: "2012-10-17",
			Statement: []awscreate.PolicyDocumentResourceStatementEntry{
				{
					Effect: "Allow",
					Action: []string{
						"s3:Get*",
					},
					Principal: map[string]any{
						"AWS": awsPrincipals,
					},
				},
				{
					Effect: "Allow",
					Action: []string{
						"s3:Put*",
					},
					Principal: map[string]any{
						"AWS": awsPrincipals[0],
					},
				},
				{
					Effect: "Allow",
					Action: []string{
						"s3:Put*",
					},
					Principal: map[string]any{
						"AWS": awsPrincipals[1],
					},
				},
			},
		}

		for _, bucketInput := range inputs.Buckets {
			policy.Statement[0].Resource = []string{fmt.Sprintf("arn:aws:s3:::%s/*", *bucketInput.Bucket)}
			// Allow the first user (client) to Put objects to the server.
			policy.Statement[1].Resource = []string{fmt.Sprintf("arn:aws:s3:::%s/to_server/*", *bucketInput.Bucket)}
			// Allow the second user (server) to Put objects to the client.
			policy.Statement[2].Resource = []string{fmt.Sprintf("arn:aws:s3:::%s/to_client/*", *bucketInput.Bucket)}
			b, err := json.Marshal(policy)
			if err != nil {
				return res, err
			}

			s := string(b)

			_, err = s3Client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
				Bucket: bucketInput.Bucket,
				Policy: &s,
			})

			if err != nil {
				return res, fmt.Errorf("failed to set bucket policy: %w", err)
			}

			_, err = s3Client.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
				Bucket: bucketInput.Bucket,
				PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
					BlockPublicAcls:       true,
					BlockPublicPolicy:     true,
					IgnorePublicAcls:      true,
					RestrictPublicBuckets: true,
				},
			})

			if err != nil {
				return res, fmt.Errorf("failed to set public access block: %w", err)
			}
		}

	}

	if len(inputs.BucketsNotifications) > 0 {
		s3Client := s3.NewFromConfig(adminCfg)

		for _, bucketNotificationInput := range inputs.BucketsNotifications {
			for j := range bucketNotificationInput.NotificationConfiguration.QueueConfigurations {
				bucketNotificationInput.NotificationConfiguration.QueueConfigurations[j].QueueArn = aws.String(queueArn(*inputs.Queues[j].QueueName))
			}
			b, err := s3Client.PutBucketNotificationConfiguration(ctx, bucketNotificationInput)
			if err != nil {
				return res, fmt.Errorf("failed to create bucket notification: %w", err)
			}
			res.BucketsNotifications = append(res.BucketsNotifications, b)
		}
	}

	return res, nil
}

func destroy(ctx context.Context, adminCfg aws.Config, inputs createInputs) error {
	if len(inputs.Buckets) > 0 {
		s3Client := s3.NewFromConfig(adminCfg)
		for _, bucketInput := range inputs.Buckets {
			if err := drainBucket(ctx, s3Client, *bucketInput.Bucket); err != nil {
				return fmt.Errorf("failed to drain bucket: %w", err)
			}
			_, err := s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucketInput.Bucket})
			if err != nil {
				if isNoSuchEntityErr(err) {
					continue
				}
				return fmt.Errorf("failed to delete bucket: %w", err)
			}
		}

	}

	if len(inputs.Users) > 0 {
		iamClient := iam.NewFromConfig(adminCfg)

		for _, userInput := range inputs.Users {
			// In the current setup there is a 1:1 mapping between users and access keys.
			accessKeys, err := iamClient.ListAccessKeys(ctx, &iam.ListAccessKeysInput{UserName: userInput.UserName})
			if err != nil {
				if isNoSuchEntityErr(err) {
					continue
				}
				return fmt.Errorf("failed to list access keys: %T %w", err, err)
			}
			for _, accessKey := range accessKeys.AccessKeyMetadata {
				_, err := iamClient.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{
					AccessKeyId: accessKey.AccessKeyId,
					UserName:    userInput.UserName,
				})
				if err != nil {
					if isNoSuchEntityErr(err) {
						continue
					}
					return fmt.Errorf("failed to delete access key: %w", err)
				}
			}

			if _, err := iamClient.DeleteUser(ctx, &iam.DeleteUserInput{UserName: userInput.UserName}); err != nil {
				if isNoSuchEntityErr(err) {
					continue
				}
				return fmt.Errorf("failed to delete user: %w", err)
			}
		}

		time.Sleep(10 * time.Second)
	}

	if len(inputs.Queues) > 0 {
		sqsClient := sqs.NewFromConfig(adminCfg)
		var queueDeleted bool
		for _, queueInput := range inputs.Queues {
			_, err := sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: queueInput.QueueName})

			if err != nil {
				if isNoSuchEntityErr(err) {
					continue
				}
				return fmt.Errorf("failed to delete queue: %w", err)
			} else {
				queueDeleted = true
			}
		}

		if queueDeleted {
			//Must  wait 60 seconds after deleting a queue before one can create another queue with the same name.
			time.Sleep(60 * time.Second)
		}
	}

	return nil
}

func drainBucket(ctx context.Context, client *s3.Client, bucket string) error {
	objects, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: &bucket,
	})

	if err != nil {
		if isNoSuchEntityErr(err) {
			return nil
		}

		return err
	}

	for _, object := range objects.Contents {
		_, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    object.Key,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

var isNoSuchEntityRe = regexp.MustCompile(`NoSuchEntity|NonExistentQueue|NoSuchBucket`)

func isNoSuchEntityErr(err error) bool {
	return err != nil && isNoSuchEntityRe.MatchString(err.Error())
}
