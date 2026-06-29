#pragma warning disable CA1711
#pragma warning disable CS1591

using System;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Common.Net;
using MediaBrowser.Controller;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Dto;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.LiveTv;
using MediaBrowser.Model.MediaInfo;
using Microsoft.Extensions.Logging;

namespace Jellyfin.LiveTv.TunerHosts
{
    public class SharedHttpStream : LiveStream, IDirectStreamProvider
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IServerApplicationHost _appHost;

        public SharedHttpStream(
            MediaSourceInfo mediaSource,
            TunerHostInfo tunerHostInfo,
            string originalStreamId,
            IFileSystem fileSystem,
            IHttpClientFactory httpClientFactory,
            ILogger logger,
            IConfigurationManager configurationManager,
            IServerApplicationHost appHost,
            IStreamHelper streamHelper)
            : base(mediaSource, tunerHostInfo, fileSystem, logger, configurationManager, streamHelper)
        {
            _httpClientFactory = httpClientFactory;
            _appHost = appHost;
            OriginalStreamId = originalStreamId;
        }

        public override async Task Open(CancellationToken openCancellationToken)
        {
            LiveStreamCancellationTokenSource.Token.ThrowIfCancellationRequested();

            var mediaSource = OriginalMediaSource;

            var url = mediaSource.Path;

            var typeName = GetType().Name;
            Logger.LogInformation("Opening {StreamType} Live stream from {Url}", typeName, url);

            using var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                openCancellationToken,
                LiveStreamCancellationTokenSource.Token);
            linkedTokenSource.CancelAfter(TimeSpan.FromSeconds(10));

            using var response = await _httpClientFactory.CreateClient(NamedClient.Default)
                .GetAsync(url, HttpCompletionOption.ResponseHeadersRead, linkedTokenSource.Token)
                .ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            await using var stream = await response.Content.ReadAsStreamAsync(linkedTokenSource.Token).ConfigureAwait(false);
            var buffer = new byte[188];
            var bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), linkedTokenSource.Token).ConfigureAwait(false);
            if (bytesRead <= 0)
            {
                Logger.LogWarning("Zero bytes read while opening stream {StreamType} from {Url}", GetType().Name, url);
                throw new EndOfStreamException(string.Format(CultureInfo.InvariantCulture, "Zero bytes copied from stream {0}", GetType().Name));
            }

            MediaSource.Path = url;
            MediaSource.Protocol = MediaProtocol.Http;
            EnableStreamSharing = false;
            DateOpened = DateTime.UtcNow;
        }

        private Task StartStreaming(HttpResponseMessage response, TaskCompletionSource<bool> openTaskCompletionSource, CancellationToken cancellationToken)
        {
            return Task.Run(
                async () =>
                {
                    try
                    {
                        Logger.LogInformation("Beginning {StreamType} stream to {FilePath}", GetType().Name, TempFilePath);
                        using (response)
                        {
                            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                            await using (stream.ConfigureAwait(false))
                            {
                                var fileStream = new FileStream(
                                    TempFilePath,
                                    FileMode.Create,
                                    FileAccess.Write,
                                    FileShare.Read,
                                    IODefaults.FileStreamBufferSize,
                                    FileOptions.Asynchronous);

                                await using (fileStream.ConfigureAwait(false))
                                {
                                    await StreamHelper.CopyToAsync(
                                        stream,
                                        fileStream,
                                        IODefaults.CopyToBufferSize,
                                        () => Resolve(openTaskCompletionSource, fileStream),
                                        cancellationToken).ConfigureAwait(false);
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException ex)
                    {
                        Logger.LogInformation("Copying of {StreamType} to {FilePath} was canceled", GetType().Name, TempFilePath);
                        openTaskCompletionSource.TrySetException(ex);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex, "Error copying live stream {StreamType} to {FilePath}", GetType().Name, TempFilePath);
                        openTaskCompletionSource.TrySetException(ex);
                    }

                    openTaskCompletionSource.TrySetResult(false);

                    EnableStreamSharing = false;
                    await DeleteTempFiles(TempFilePath).ConfigureAwait(false);
                },
                CancellationToken.None);
        }

        private void Resolve(TaskCompletionSource<bool> openTaskCompletionSource, FileStream stream)
        {
            stream.Flush(true);
            if (!File.Exists(TempFilePath) || new FileInfo(TempFilePath).Length == 0)
            {
                return;
            }

            DateOpened = DateTime.UtcNow;
            openTaskCompletionSource.TrySetResult(true);
        }
    }
}
