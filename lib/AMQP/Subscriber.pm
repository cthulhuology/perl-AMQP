package AMQP::Subscriber;

use Mojo::Base -base;
use AnyEvent::RabbitMQ;
use Sys::Hostname;

has 'debug' => 1;
has 'host' => 'localhost';
has 'port' => 5672;
has 'user' => 'guest';
has 'password' => 'guest';
has 'vhost' => '/';
has 'timeout' => 1;
has 'heartbeat' => 30;
has 'exchange' => 'test';
has 'type' => 'topic';
has 'key' => '#';
has 'queue' => 'test';
has 'rabbit';
has 'connection';
has 'channel';
has 'status';
has 'tag' => $ENV{LOGNAME} . "@" . hostname;
has 'callback';

sub amqp {
	my ($self,$url) = @_;
	$url =~ /(?:amqp:\/\/)*(?<hostname>[^:\/]+):(?<port>\d+)\/(?<vhost>[^\/]*)/;
	$self->host($+{'host'} || 'localhost');
	$self->port($+{'port'} || 5672);
	$self->vhost($+{'vhost'} || '/');
	say "amqp://" . $self->host . ":" . $self->port . $self->vhost if $self->debug;
}

sub attach {
	my $self = shift;
	$self->useragent(Mojo::UserAgent->new);
	$self->status(AnyEvent->condvar);
	$self->rabbit(AnyEvent::RabbitMQ->new);
	$self->rabbit->load_xml_spec();
	$self->rabbit->connect(
		host => $self->host,
		port => $self->port,
		user => $self->user,
		pass => $self->password,
		vhost => $self->vhost,
		timeout => $self->timeout,
		tune => { heartbeat => $self->heartbeat },
		on_success => sub {
			say "Connected to amqp://" . $self->host . ":" . $self->port . $self->vhost if $self->debug;
			$self->connection(shift);
			$self->connection->open_channel(
				on_failure => $self->status,
				on_close => sub {
					say "Channel closed" if $self->debug;
					$self->status->send;
				},
				on_success => sub {
					say "Opened channel" if $self->debug;
					$self->channel(shift);
					$self->channel->declare_exchange(
						exchange => $self->exchange,
						type => $self->type,
						auto_delete => 1,
						on_failure => $self->status,
						on_success => sub {
							say "Declared exchange " . $self->exchange if $self->debug;
							$self->channel->declare_queue(
								queue => $self->queue,
								auto_delete => 1,
								on_failure => $self->status,
								on_success => sub {
									say "Declared queue " . $self->queue if $self->debug;
									$self->channel->bind_queue(
										queue => $self->queue,
										exchange => $self->exchange,
										routing_key => $self->key,
										on_failure => $self->status,
										on_success => sub {
											say "Bound " . $self->queue . " to " . $self->exchange . " " . $self->key if $self->debug;
											$self->channel->consume(
												consumer_tag => $self->tag,
												on_success => sub {
													say 'Consuming from ' . $self->queue if $self->debug;
												},
												on_consume => sub {
													my $msg = shift;
													$self->callback->($self,$msg);
												},
												on_cancel => sub {
													say "Consumption canceled" if $self->debug;
													$self->status->send;
												},
												on_failure => $self->status,
											);
										}
									);
								}
							);
						}
					);
				},
			);
		},
		on_failure => $self->status,
		on_read_failure =>  sub {
			say "Failed to read" if $self->debug;
			$self->status->send;
		},
		on_return => sub {
			say "Failed to send" if $self->debug;
			$self->status->send;
		},
		on_close => sub {
			say "Connection closed" if $self->debug;
			$self->status->send;
		}
	);
	$self->status->recv;
}
		

1;
